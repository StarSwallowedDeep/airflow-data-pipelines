from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from psycopg2 import sql
import pandas as pd
import os

POSTGRES_CONN_ID = "postgres_db"
SNOWFLAKE_CONN_ID = "snowflake_conn"
SNOWFLAKE_SCHEMA = "KJH"
STAGE_NAME = "MY_STAGE"
TMP_DIR = "/tmp"
CHUNK_SIZE = 50_000

# 풀로드가 끝나면 바로 실행을 걸어줄 CDC DAG의 dag_id
# CDC_DAG_ID = "postgres_to_snowflake_cdc"

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
    return TYPE_MAP.get(pg_type, "STRING")

def quote_sf_identifier(name: str) -> str:
    # Snowflake identifier SQL 인젝션 방어용
    return '"' + name.replace('"', '""') + '"'

def qualified_table(table_name: str) -> str:
    # 스키마.테이블 이름을 안전하게 조합
    return f"{quote_sf_identifier(SNOWFLAKE_SCHEMA)}.{quote_sf_identifier(table_name)}"

@dag(
    dag_id="postgres_to_snowflake_full_load",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=5,
    default_args=DEFAULT_TASK_ARGS,
    tags=["full_load", "postgres_to_snowflake"],
)
def postgres_to_snowflake_full_load():

    # -----------------------------
    # 1. TABLE LIST
    # -----------------------------
    @task
    def get_tables():
        pg = PostgresHook(POSTGRES_CONN_ID)
        return [
            r[0]
            for r in pg.get_records(
                """
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public'
                """
            )
        ]

    # -----------------------------
    # 2. CREATE TABLE (운영 + staging)
    # -----------------------------
    @task
    def create_table(table_name):
        pg = PostgresHook(POSTGRES_CONN_ID)
        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

        cols = pg.get_records(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
            """,
            parameters=(table_name,),
        )

        if not cols:
            raise ValueError(f"No columns found for table {table_name}")

        ddl = ", ".join(
            f"{quote_sf_identifier(col[0])} {map_type(col[1])}" for col in cols
        )
        staging_name = f"{table_name}_staging"

        sf.run(f"CREATE TABLE IF NOT EXISTS {qualified_table(table_name)} ({ddl})")
        sf.run(f"CREATE OR REPLACE TABLE {qualified_table(staging_name)} ({ddl})")

        # table_name뿐 아니라 컬럼 목록을 같이 넘겨서 export_parquet의 중복 조회 방지
        col_names = [c[0] for c in cols]
        return {"table_name": table_name, "columns": col_names}

    # -----------------------------
    # 3. EXPORT PARQUET (청크 단위)
    # -----------------------------
    @task
    def export_parquet(payload):
        table_name = payload["table_name"]
        col_names = payload["columns"]

        pg = PostgresHook(POSTGRES_CONN_ID)
        engine = pg.get_sqlalchemy_engine()

        file_path = f"{TMP_DIR}/{table_name}.parquet"

        query = sql.SQL("SELECT * FROM {}").format(
            sql.Identifier(table_name)
        ).as_string(pg.get_conn())

        import pyarrow as pa
        import pyarrow.parquet as pq

        writer = None
        try:
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
            pd.DataFrame(columns=col_names).to_parquet(file_path, index=False, engine="pyarrow")

        return {"table": table_name, "file": file_path}

    # -----------------------------
    # 4. UPLOAD TO STAGE
    # -----------------------------
    @task
    def upload(payload):
        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
        sf.run(f"PUT file://{payload['file']} @{STAGE_NAME} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
        return payload

    # -----------------------------
    # 5. COPY INTO STAGING -> SWAP
    # -----------------------------
    @task
    def copy_into(payload):
        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
        table_name = payload["table"]
        staging_name = f"{table_name}_staging"

        sf.run(f"""
            COPY INTO {qualified_table(staging_name)}
            FROM @{STAGE_NAME}/{table_name}.parquet
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE
        """)

        # 실패해도 운영 테이블은 유지되는 원자적 교체
        sf.run(f"""
            ALTER TABLE {qualified_table(table_name)}
            SWAP WITH {qualified_table(staging_name)}
        """)

        return table_name

    # -----------------------------
    # 6. VALIDATE
    # -----------------------------
    @task
    def validate(table_name):
        pg = PostgresHook(POSTGRES_CONN_ID)
        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

        pg_count = pg.get_first(
            sql.SQL("SELECT COUNT(*) FROM {}").format(
                sql.Identifier(table_name)
            ).as_string(pg.get_conn())
        )[0]

        sf_count = sf.get_first(f"SELECT COUNT(*) FROM {qualified_table(table_name)}")[0]

        if pg_count != sf_count:
            raise ValueError(f"{table_name} mismatch: PG={pg_count}, SF={sf_count}")

        return table_name

    # -----------------------------
    # 7. CLEANUP (항상 실행)
    # -----------------------------
    @task(trigger_rule="all_done")
    def cleanup(table_name):
        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
        staging_name = f"{table_name}_staging"

        sf.run(f"REMOVE @{STAGE_NAME}/{table_name}.parquet")
        sf.run(f"DROP TABLE IF EXISTS {qualified_table(staging_name)}")

        local_file = f"{TMP_DIR}/{table_name}.parquet"
        if os.path.exists(local_file):
            os.remove(local_file)

        return f"{table_name} cleaned"

    # -----------------------------
    # FLOW
    # -----------------------------
    tables = get_tables()

    with TaskGroup("etl_pipeline"):
        t1 = create_table.expand(table_name=tables)
        t2 = export_parquet.expand(payload=t1)
        t3 = upload.expand(payload=t2)
        t4 = copy_into.expand(payload=t3)
        t5 = validate.expand(table_name=t4)
        cleanup.expand(table_name=t5)
        # cleanup_task = cleanup.expand(table_name=t5)

    # -----------------------------
    # 8. 풀로드 완료 -> CDC DAG 트리거
    #    (이후 증분 동기화는 CDC DAG 자체 스케줄이 이어받음)
    # -----------------------------
    # trigger_cdc = TriggerDagRunOperator(
    #     task_id="trigger_cdc_dag",
    #     trigger_dag_id=CDC_DAG_ID,
    #     wait_for_completion=False,
    #     reset_dag_run=True,  # 같은 execution_date로 재트리거 시 기존 run 재사용
    # )
 
    # cleanup_task >> trigger_cdc


postgres_to_snowflake_full_load()