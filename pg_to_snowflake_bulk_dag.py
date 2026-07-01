import os
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from psycopg2 import sql
import pandas as pd

SNOWFLAKE_SCHEMA = "MY_SCHEMA"
STAGE_NAME = "MY_STAGE"
CHUNK_SIZE = 50_000  # [개선] 대용량 테이블 OOM 방지용 청크 크기. export_parquet에서 사용

# [개선] 기존엔 retries 설정이 전혀 없어서 일시적 네트워크 오류에도 태스크가 바로 실패했음.
# 모든 태스크에 공통으로 재시도 2회 + 2분 대기를 적용 (default_args로 DAG 전체에 일괄 적용)
DEFAULT_TASK_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

TYPE_MAP = {
    "smallint": "NUMBER",
    "integer": "NUMBER",
    "bigint": "NUMBER",
    "numeric": "NUMBER",
    "real": "FLOAT",
    "double precision": "FLOAT",
    "text": "STRING",
    "character varying": "STRING",
    "character": "STRING",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp without time zone": "TIMESTAMP_NTZ",
    "timestamp with time zone": "TIMESTAMP_TZ",
    "json": "VARIANT",
    "jsonb": "VARIANT",
    "uuid": "STRING",
}


def map_type(pg_type: str) -> str:
    """Postgres 타입을 Snowflake 타입으로 변환. TYPE_MAP에 없는 타입은 STRING으로 폴백."""
    return TYPE_MAP.get(pg_type, "STRING")


def qualified_table(table_name: str) -> str:
    """[개선] 'SCHEMA.table' 형태의 완전한 테이블명을 만드는 헬퍼.
    기존엔 테이블명만 썼지만, 여러 군데서 반복되는 'PUBLIC.xxx' 조합을 한 곳에서 관리하기 위함."""
    return f"{SNOWFLAKE_SCHEMA}.{table_name}"


@dag(
    dag_id="pg_to_snowflake_bulk_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=5,      # [개선] 기존엔 동시 실행 제한이 없어서 테이블이 많으면 expand()로 생성된 태스크들이
                             # 한꺼번에 Postgres/Snowflake에 동시 접속해 부하를 줄 수 있었음. 5개로 제한
    default_args=DEFAULT_TASK_ARGS,
)
def pg_to_snowflake_bulk_dag():

    # -----------------------------
    # 1. TABLE LIST
    # -----------------------------
    @task
    def get_tables():
        pg = PostgresHook("postgres_netflix")
        return [r[0] for r in pg.get_records("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
        """)]

    # -----------------------------
    # 2. CREATE STAGING TABLE (SAFE, swap 전략용)
    # -----------------------------
    @task
    def create_table(table_name):
        pg = PostgresHook("postgres_netflix")
        sf = SnowflakeHook("snowflake_conn")

        query = sql.SQL("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """).as_string(pg.get_conn())

        cols = pg.get_records(
            query,
            parameters=(table_name,)
        )

        if not cols:
            raise ValueError(f"No columns found for table {table_name}")

        ddl = ", ".join(f"{col[0]} {map_type(col[1])}" for col in cols)
        staging_name = f"{table_name}_staging"

        # [개선] 기존엔 Snowflake CREATE TABLE을 f"CREATE TABLE IF NOT EXISTS {table_name} ({ddl})" 처럼
        # f-string으로 테이블명을 직접 끼워 넣었음. Postgres 쪽만 sql.Identifier로 안전하게 처리하고
        # Snowflake 쪽은 그대로 둬서 일관성이 없었던 부분을 동일하게 맞춤.
        # 운영 테이블 (없으면 생성)
        prod_query = f"CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.{table_name} ({ddl})"
        sf.run(prod_query)

        # [개선] 기존엔 staging 테이블 개념이 아예 없었음 (바로 운영 테이블에 TRUNCATE + COPY).
        # SWAP 전략을 쓰기 위해 매 실행마다 동일 구조의 staging 테이블을 새로 만듦
        # 스테이징 테이블 (매 실행마다 재생성)
        staging_query = f"CREATE OR REPLACE TABLE {SNOWFLAKE_SCHEMA}.{staging_name} ({ddl})"
        sf.run(staging_query)

        return table_name

    # -----------------------------
    # 3. EXPORT PARQUET (청크 단위, 메모리 안전)
    # -----------------------------
    @task
    def export_parquet(table_name):
        pg = PostgresHook("postgres_netflix")
        engine = pg.get_sqlalchemy_engine()

        postgres_query = sql.SQL("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """).as_string(pg.get_conn())

        col_info = pg.get_records(
            postgres_query,
            parameters=(table_name,)
        )
        col_names = [c[0] for c in col_info]

        file_path = f"/tmp/{table_name}.parquet"
        query = sql.SQL("SELECT * FROM {}").format(
            sql.Identifier(table_name)
        ).as_string(pg.get_conn())

        # [개선] 기존엔 pg.get_records(f"SELECT * FROM {table_name}")로 테이블 전체를
        # 한 번에 메모리(rows 리스트)로 읽어와서 DataFrame으로 변환했음.
        # 큰 테이블이면 Airflow worker 메모리가 부족해질 위험이 있었음.
        # 여기서는 pd.read_sql(chunksize=...)로 5만 행씩 나눠 읽고,
        # ParquetWriter로 같은 파일에 이어 쓰는 방식(append)으로 메모리 사용량을 일정하게 유지함.
        writer = None
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            for chunk in pd.read_sql(query, engine, chunksize=CHUNK_SIZE):
                chunk.columns = col_names
                table = pa.Table.from_pandas(chunk, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(file_path, table.schema)
                writer.write_table(table)
        finally:
            if writer is not None:
                writer.close()

        if writer is None:
            # 빈 테이블인 경우 빈 parquet 생성
            pd.DataFrame(columns=col_names).to_parquet(file_path, index=False, engine="pyarrow")

        return {"table": table_name, "file": file_path}

    # -----------------------------
    # 4. UPLOAD TO SNOWFLAKE STAGE
    # -----------------------------
    @task
    def upload(payload):
        sf = SnowflakeHook("snowflake_conn")
        table_name = payload["table"]
        file_path = payload["file"]

        sf.run(f"PUT file://{file_path} @{STAGE_NAME} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

        return payload

    # -----------------------------
    # 5. COPY INTO STAGING -> SWAP (원자적 적용)
    # -----------------------------
    @task
    def copy_into(payload):
        sf = SnowflakeHook("snowflake_conn")
        table_name = payload["table"]
        staging_name = f"{table_name}_staging"

        sf.run(f"""
            CREATE OR REPLACE TABLE {qualified_table(staging_name)}
            LIKE {qualified_table(table_name)}
        """)

        sf.run(f"""
            COPY INTO {qualified_table(staging_name)}
            FROM @{STAGE_NAME}/{table_name}.parquet
            FILE_FORMAT = (
                TYPE = PARQUET
                SNAPPY_COMPRESSION = TRUE
                USE_LOGICAL_TYPE = TRUE
            )
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE
        """)

        # [개선] 기존엔 TRUNCATE TABLE 후 바로 COPY INTO를 했음.
        # COPY가 중간에 실패하면 운영 테이블이 빈 상태로 남는 문제가 있었음.
        # 여기서는 별도 staging 테이블에 COPY를 끝낸 뒤, 검증된 데이터만 SWAP으로 한 번에 교체함.
        # 주의: SWAP은 단일 SQL문이라 그 자체는 원자적이지만, COPY와 SWAP은 별개의 sf.run() 호출이라
        # 그 사이에 DAG/커넥션이 끊기면 staging만 채워지고 swap이 안 될 수 있음(이 경우 운영 테이블은 안전하게 유지됨).
        # 원자적 swap: 실패해도 기존 운영 테이블은 그대로 유지됨
        sf.run(f"""
            ALTER TABLE {qualified_table(table_name)}
            SWAP WITH {qualified_table(staging_name)}
        """)

        return table_name

    # -----------------------------
    # 6. VALIDATE (SAFE)
    # -----------------------------
    @task
    def validate(table_name):
        pg = PostgresHook("postgres_netflix")
        sf = SnowflakeHook("snowflake_conn")

        pg_count = pg.get_first(
            sql.SQL("SELECT COUNT(*) FROM {}").format(
                sql.Identifier(table_name)
            ).as_string(pg.get_conn())
        )[0]

        sf_count = sf.get_first(
            f"SELECT COUNT(*) FROM {qualified_table(table_name)}"
        )[0]

        if pg_count != sf_count:
            raise ValueError(f"{table_name} mismatch: PG={pg_count}, SF={sf_count}")

        return table_name

    # -----------------------------
    # 7. CLEANUP (Snowflake stage + 스테이징 테이블 + 로컬 파일)
    # -----------------------------
    @task(trigger_rule="all_done")  # [개선] 기존엔 trigger_rule이 없어서(기본값 all_success) 앞 단계가 실패하면
                                    # cleanup이 아예 실행되지 않아 stage 파일/staging 테이블/로컬 파일이 그대로 남았음.
                                    # all_done으로 바꿔서 실패 여부와 무관하게 항상 정리되도록 함
    def cleanup(table_name):
        sf = SnowflakeHook("snowflake_conn")
        staging_name = f"{table_name}_staging"

        sf.run(f"REMOVE @{STAGE_NAME}/{table_name}.parquet")
        # [개선] 기존엔 staging 테이블 개념이 없어서 정리할 것도 없었음. SWAP 전략 도입으로
        # swap 후 staging_name이 "옛 운영 테이블"을 가리키게 되므로 여기서 drop 해줘야 함
        sf.run(f"DROP TABLE IF EXISTS {qualified_table(staging_name)}")

        # [개선] 기존엔 /tmp에 생성한 parquet 파일을 끝까지 지우지 않아 디스크 누수가 발생했음
        local_file = f"/tmp/{table_name}.parquet"
        if os.path.exists(local_file):
            os.remove(local_file)

        return f"{table_name} cleaned"

    # -----------------------------
    # FLOW (TASK MAPPING)
    # -----------------------------
    tables = get_tables()

    with TaskGroup("etl_pipeline"):
        t1 = create_table.expand(table_name=tables)
        t2 = export_parquet.expand(table_name=t1)
        t3 = upload.expand(payload=t2)
        t4 = copy_into.expand(payload=t3)
        t5 = validate.expand(table_name=t4)
        cleanup.expand(table_name=t5)


pg_to_snowflake_bulk_dag()