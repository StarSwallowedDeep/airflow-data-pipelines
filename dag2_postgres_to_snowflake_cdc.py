from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
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

UPDATED_AT_CANDIDATES = ["updated_at", "modified_at", "updated_on", "last_modified"]
DELETE_FLAG_CANDIDATES = ["is_deleted", "deleted", "deleted_at"]

CDC_CONTROL_TABLE = "CDC_CONTROL"
DEFAULT_WATERMARK = "1970-01-01 00:00:00"

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
    # Snowflake 스키마와 테이블명을 안전하게 조합
    return f"{quote_sf_identifier(SNOWFLAKE_SCHEMA)}.{quote_sf_identifier(table_name)}"

@dag(
    dag_id="postgres_to_snowflake_cdc",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False, 
    max_active_runs=1,   # 이전 실행이 안 끝났으면 다음 스케줄 실행을 띄우지 않음 (staging/control row 레이스 방지)
    max_active_tasks=5,  # 한 실행 안에서 테이블별 병렬 처리 개수 제한
    default_args=DEFAULT_TASK_ARGS,
    tags=["cdc", "postgres_to_snowflake"],
)
def postgres_to_snowflake_cdc():

    # -----------------------------
    # 0. CONTROL TABLE 준비
    # -----------------------------
    @task
    def ensure_control_table():
        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
        sf.run(f"""
            CREATE TABLE IF NOT EXISTS {qualified_table(CDC_CONTROL_TABLE)} (
                TABLE_NAME STRING,
                UPDATED_AT_COL STRING,
                PK_COLUMNS STRING,
                DELETE_FLAG_COL STRING,
                LAST_WATERMARK TIMESTAMP_NTZ,
                LAST_RUN_AT TIMESTAMP_NTZ
            )
        """)

    # -----------------------------
    # 1. CDC 대상 테이블 판별
    #    (PK + updated_at류 컬럼이 모두 있는 테이블만 대상)
    # -----------------------------
    @task
    def get_cdc_tables():
        pg = PostgresHook(POSTGRES_CONN_ID)

        tables = [
            r[0]
            for r in pg.get_records(
                """
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public'
                """
            )
        ]

        eligible = []
        for table_name in tables:
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
                continue
            col_names = [c[0] for c in cols]

            updated_at_col = next(
                (c for c in UPDATED_AT_CANDIDATES if c in col_names), None
            )
            delete_flag_col = next(
                (c for c in DELETE_FLAG_CANDIDATES if c in col_names), None
            )

            pk_rows = pg.get_records(
                """
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                WHERE tc.table_schema = 'public'
                  AND tc.table_name = %s
                  AND tc.constraint_type = 'PRIMARY KEY'
                ORDER BY kcu.ordinal_position
                """,
                parameters=(table_name,),
            )
            pk_columns = [r[0] for r in pk_rows]

            if not updated_at_col or not pk_columns:
                # PK나 변경시각 컬럼이 없으면 이 방식으로는 CDC 불가 -> 건너뜀
                continue

            eligible.append(
                {
                    "table_name": table_name,
                    "columns": col_names,
                    "column_types": {c[0]: c[1] for c in cols},
                    "pk_columns": pk_columns,
                    "updated_at_col": updated_at_col,
                    "delete_flag_col": delete_flag_col,
                }
            )

        return eligible

    # -----------------------------
    # 2. 대상/스테이징 테이블 준비 + 스키마 진화 + CONTROL row 초기화
    # -----------------------------
    @task
    def create_table(cfg):
        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
        table_name = cfg["table_name"]
        columns = cfg["columns"]
        column_types = cfg["column_types"]

        ddl = ", ".join(
            f"{quote_sf_identifier(col)} {map_type(column_types[col])}" for col in columns
        )
        staging_name = f"{table_name}_cdc_staging"

        exists = sf.get_first(
            """
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table)s
            """,
            parameters={"schema": SNOWFLAKE_SCHEMA, "table": table_name},
        )[0]

        if not exists:
            sf.run(f"CREATE TABLE IF NOT EXISTS {qualified_table(table_name)} ({ddl})")
        else:
            # 스키마 진화: Postgres에 새로 생긴 컬럼을 Snowflake에도 반영
            existing_cols = {
                r[0]
                for r in sf.get_records(
                    """
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table)s
                    """,
                    parameters={"schema": SNOWFLAKE_SCHEMA, "table": table_name},
                )
            }
            missing_cols = [c for c in columns if c not in existing_cols]
            for col in missing_cols:
                sf.run(
                    f"ALTER TABLE {qualified_table(table_name)} "
                    f"ADD COLUMN {quote_sf_identifier(col)} {map_type(column_types[col])}"
                )

        sf.run(f"CREATE TABLE IF NOT EXISTS {qualified_table(staging_name)} ({ddl})")
        sf.run(f"TRUNCATE TABLE {qualified_table(staging_name)}")

        control_exists = sf.get_first(
            f"""
            SELECT COUNT(*) FROM {qualified_table(CDC_CONTROL_TABLE)}
            WHERE TABLE_NAME = %s
            """,
            parameters=(table_name,),
        )
        if control_exists[0] == 0:
            pk_str = ",".join(cfg["pk_columns"])
            sf.run(
                f"""
                INSERT INTO {qualified_table(CDC_CONTROL_TABLE)}
                (TABLE_NAME, UPDATED_AT_COL, PK_COLUMNS, DELETE_FLAG_COL, LAST_WATERMARK, LAST_RUN_AT)
                VALUES (%(table_name)s, %(updated_at_col)s, %(pk_cols)s, %(delete_flag_col)s, %(watermark)s, NULL)
                """,
                parameters={
                    "table_name": table_name,
                    "updated_at_col": cfg["updated_at_col"],
                    "pk_cols": pk_str,
                    "delete_flag_col": cfg["delete_flag_col"] or "",
                    "watermark": DEFAULT_WATERMARK,
                },
            )

        return cfg

    # -----------------------------
    # 3. 워터마크 이후 변경분만 EXPORT
    # -----------------------------
    @task
    def export_parquet(cfg):
        table_name = cfg["table_name"]
        col_names = cfg["columns"]
        updated_at_col = cfg["updated_at_col"]

        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
        watermark_row = sf.get_first(
            f"""
            SELECT LAST_WATERMARK FROM {qualified_table(CDC_CONTROL_TABLE)}
            WHERE TABLE_NAME = %s
            """,
            parameters=(table_name,),
        )
        watermark = watermark_row[0] if watermark_row and watermark_row[0] else DEFAULT_WATERMARK

        pg = PostgresHook(POSTGRES_CONN_ID)
        engine = pg.get_sqlalchemy_engine()
        conn = pg.get_conn()

        query = sql.SQL(
            "SELECT {cols} FROM {table} WHERE {watermark_col} > %(watermark)s ORDER BY {watermark_col}"
        ).format(
            cols=sql.SQL(", ").join(sql.Identifier(c) for c in col_names),
            table=sql.Identifier(table_name),
            watermark_col=sql.Identifier(updated_at_col),
        ).as_string(conn)

        file_path = f"{TMP_DIR}/{table_name}_cdc.parquet"

        import pyarrow as pa
        import pyarrow.parquet as pq

        writer = None
        max_watermark = None
        row_count = 0
        try:
            for chunk in pd.read_sql(
                query, engine, params={"watermark": watermark}, chunksize=CHUNK_SIZE
            ):
                chunk.columns = col_names
                row_count += len(chunk)
                chunk_max = chunk[updated_at_col].max()
                if pd.notnull(chunk_max) and (max_watermark is None or chunk_max > max_watermark):
                    max_watermark = chunk_max

                table = pa.Table.from_pandas(chunk, preserve_index=False)

                if writer is None:
                    writer = pq.ParquetWriter(
                        file_path,
                        table.schema,
                        coerce_timestamps="us",
                        allow_truncated_timestamps=True,
                    )

                writer.write_table(table)
        finally:
            if writer is not None:
                writer.close()

        has_data = row_count > 0
        if not has_data and os.path.exists(file_path):
            os.remove(file_path)

        return {
            **cfg,
            "file": file_path,
            "has_data": has_data,
            "row_count": row_count,
            "new_watermark": str(max_watermark) if max_watermark is not None else watermark,
        }

    # -----------------------------
    # 4. UPLOAD (변경분 있을 때만)
    # -----------------------------
    @task
    def upload(payload):
        if not payload["has_data"]:
            return payload
        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
        sf.run(
            f"PUT file://{payload['file']} @{STAGE_NAME} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        return payload

    # -----------------------------
    # 5. COPY INTO STAGING
    # -----------------------------
    @task
    def copy_into_staging(payload):
        table_name = payload["table_name"]

        if not payload["has_data"]:
            return payload

        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
        staging_name = f"{table_name}_cdc_staging"
        source_file = os.path.basename(payload["file"])

        sf.run(f"TRUNCATE TABLE {qualified_table(staging_name)}")

        sf.run(f"""
            COPY INTO {qualified_table(staging_name)}
            FROM @{STAGE_NAME}/{source_file}
            FILE_FORMAT = (
                TYPE = PARQUET
                USE_LOGICAL_TYPE = TRUE
            )
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE
        """)

        return payload

    # -----------------------------
    # 6. VALIDATE (MERGE 전에 유실/중복 여부 확인 -> 게이트 역할)
    # -----------------------------
    @task
    def validate(payload):
        table_name = payload["table_name"]

        if not payload["has_data"]:
            return payload

        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
        staging_name = f"{table_name}_cdc_staging"

        staged_count = sf.get_first(
            f"SELECT COUNT(*) FROM {qualified_table(staging_name)}"
        )[0]

        if staged_count != payload["row_count"]:
            raise ValueError(
                f"[PRE-MERGE VALIDATE FAIL] {table_name}: "
                f"extracted={payload['row_count']}, staged_in_snowflake={staged_count}"
            )

        return payload

    # -----------------------------
    # 7. MERGE (UPSERT / soft-DELETE)
    # -----------------------------
    @task
    def merge_cdc(payload):
        table_name = payload["table_name"]

        if not payload["has_data"]:
            return {"table_name": table_name, "new_watermark": payload["new_watermark"]}

        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
        staging_name = f"{table_name}_cdc_staging"
        pk_columns = payload["pk_columns"]
        columns = payload["columns"]
        delete_flag_col = payload.get("delete_flag_col")

        on_clause = " AND ".join(
            f"t.{quote_sf_identifier(pk)} = s.{quote_sf_identifier(pk)}" for pk in pk_columns
        )
        update_set = ", ".join(
            f"{quote_sf_identifier(c)} = s.{quote_sf_identifier(c)}"
            for c in columns
            if c not in pk_columns
        )
        insert_cols = ", ".join(quote_sf_identifier(c) for c in columns)
        insert_vals = ", ".join(f"s.{quote_sf_identifier(c)}" for c in columns)

        if delete_flag_col:
            merge_sql = f"""
                MERGE INTO {qualified_table(table_name)} t
                USING {qualified_table(staging_name)} s
                ON {on_clause}
                WHEN MATCHED AND s.{quote_sf_identifier(delete_flag_col)} = TRUE THEN DELETE
                WHEN MATCHED THEN UPDATE SET {update_set}
                WHEN NOT MATCHED AND (s.{quote_sf_identifier(delete_flag_col)} IS NULL
                    OR s.{quote_sf_identifier(delete_flag_col)} = FALSE) THEN
                    INSERT ({insert_cols}) VALUES ({insert_vals})
            """
        else:
            merge_sql = f"""
                MERGE INTO {qualified_table(table_name)} t
                USING {qualified_table(staging_name)} s
                ON {on_clause}
                WHEN MATCHED THEN UPDATE SET {update_set}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """

        sf.run(merge_sql)

        return {"table_name": table_name, "new_watermark": payload["new_watermark"]}

    # -----------------------------
    # 8. 워터마크 갱신 (MERGE 성공 시에만 실행 -> 실패하면 재처리 가능)
    # -----------------------------
    @task
    def update_watermark(result):
        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
        sf.run(
            f"""
            UPDATE {qualified_table(CDC_CONTROL_TABLE)}
            SET LAST_WATERMARK = %(watermark)s, LAST_RUN_AT = CURRENT_TIMESTAMP()
            WHERE TABLE_NAME = %(table_name)s
            """,
            parameters={
                "watermark": result["new_watermark"],
                "table_name": result["table_name"],
            },
        )
        return result["table_name"]

    # -----------------------------
    # 9. CLEANUP (항상 실행 - watermark 갱신과 분리된 별도 태스크)
    #
    # zip()으로 테이블별 1:1 의존성을 구성하여,
    # 각 cleanup이 자신의 merge/update_watermark 완료 후 즉시 실행되도록 함.
    # -----------------------------
    @task(trigger_rule="all_done")
    def cleanup(zipped):
        cfg, _merge_result = zipped
        table_name = cfg["table_name"]

        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
        staging_name = f"{table_name}_cdc_staging"

        sf.run(f"REMOVE @{STAGE_NAME}/{table_name}_cdc.parquet")
        sf.run(f"DROP TABLE IF EXISTS {qualified_table(staging_name)}")

        local_file = f"{TMP_DIR}/{table_name}_cdc.parquet"
        if os.path.exists(local_file):
            os.remove(local_file)

        return f"{table_name} cdc cleaned"

    # -----------------------------
    # 실습: Sensor 개념 확인용
    # 특정 테이블이 Postgres에 존재하는지 확인
    # -----------------------------
    # @task.sensor(mode="reschedule", timeout=60 * 5, poke_interval=20)
    # def wait_for_new_changes():
    #     pg = PostgresHook(POSTGRES_CONN_ID)
    #     exists = pg.get_first(
    #         """
    #         SELECT COUNT(*) FROM information_schema.tables
    #         WHERE table_schema = 'public' AND table_name = 'sensor_test'
    #         """
    #     )[0]
    #     print(f"sensor_test 테이블 존재 여부: {exists > 0}")
    #     return exists > 0

    # wait_for_changes_task = wait_for_new_changes()

    # -----------------------------
    # FLOW
    # -----------------------------
    control_ready = ensure_control_table()
    tables = get_cdc_tables()
    control_ready >> tables

    # wait_for_changes_task >> control_ready

    with TaskGroup("cdc_pipeline"):
        t1 = create_table.expand(cfg=tables)
        t2 = export_parquet.expand(cfg=t1)
        t3 = upload.expand(payload=t2)
        t4 = copy_into_staging.expand(payload=t3)
        t5 = validate.expand(payload=t4)
        t6 = merge_cdc.expand(payload=t5)
        t7 = update_watermark.expand(result=t6)

        # t1(config)과 t7(merge/watermark 결과)을 인덱스 기준으로 짝지어서
        # cleanup 인스턴스별로 독립된 의존성을 갖게 함.
        # (Airflow 2.8+ XComArg.zip() 필요)
        zipped_for_cleanup = t1.zip(t7)
        cleanup_task = cleanup.expand(zipped=zipped_for_cleanup)


postgres_to_snowflake_cdc()