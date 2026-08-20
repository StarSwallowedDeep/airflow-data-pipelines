from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta
from psycopg2 import sql

import pandas as pd
import os


# ============================================================
# CONFIG
# ============================================================
POSTGRES_CONN_ID = "postgres_db"
SNOWFLAKE_CONN_ID = "snowflake_conn"

SNOWFLAKE_SCHEMA = "KJH"
STAGE_NAME = "MY_STAGE"

TMP_DIR = "/tmp"
CHUNK_SIZE = 50_000

# 모니터링 테이블
MONITORING_TABLE = "MIGRATION_MONITORING"

DEFAULT_TASK_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


# ============================================================
# PostgreSQL → Snowflake TYPE MAP
# ============================================================
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
    """
    Snowflake identifier를 안전하게 quoting.
    """
    return '"' + name.replace('"', '""') + '"'


def qualified_table(table_name: str) -> str:
    """
    KJH.TABLE_NAME 형태로 반환
    """
    return (
        f"{quote_sf_identifier(SNOWFLAKE_SCHEMA)}."
        f"{quote_sf_identifier(table_name)}"
    )


# ============================================================
# DAG
# ============================================================
@dag(
    dag_id="full_load_monitoring",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=5,
    default_args=DEFAULT_TASK_ARGS,
    tags=[
        "full_load",
        "postgres_to_snowflake",
    ],
)
def full_load_monitoring():

    # ========================================================
    # 0. MONITORING TABLE
    # ========================================================
    @task
    def ensure_monitoring_table():
        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
        sf.run(
            f"""
            CREATE TABLE IF NOT EXISTS
            {qualified_table(MONITORING_TABLE)}
            (
                RUN_ID VARCHAR,
                PIPELINE_TYPE VARCHAR,
                TABLE_NAME VARCHAR,

                START_TIME TIMESTAMP_NTZ,
                END_TIME TIMESTAMP_NTZ,

                STATUS VARCHAR,

                PG_COUNT NUMBER,
                SF_COUNT NUMBER,
                COUNT_DIFF NUMBER,

                EXTRACTED_COUNT NUMBER,
                STAGED_COUNT NUMBER,

                WATERMARK TIMESTAMP_NTZ,

                ERROR_MESSAGE VARCHAR
            )
            """
        )

    # ========================================================
    # 1. TABLE LIST
    # ========================================================
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

    # ========================================================
    # 2. CREATE TABLE
    # ========================================================
    @task
    def create_table(table_name):
        pg = PostgresHook(POSTGRES_CONN_ID)
        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
        cols = pg.get_records(
            """
            SELECT
                column_name,
                data_type
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
            """,
            parameters=(table_name,),
        )

        if not cols:
            raise ValueError(
                f"No columns found for table {table_name}"
            )

        ddl = ", ".join(
            f"{quote_sf_identifier(col[0])} {map_type(col[1])}"
            for col in cols
        )
        staging_name = f"{table_name}_staging"

        sf.run(
            f"""
            CREATE TABLE IF NOT EXISTS
            {qualified_table(table_name)}
            (
                {ddl}
            )
            """
        )

        sf.run(
            f"""
            CREATE OR REPLACE TABLE
            {qualified_table(staging_name)}
            (
                {ddl}
            )
            """
        )

        col_names = [
            c[0]
            for c in cols
        ]
        return {
            "table_name": table_name,
            "columns": col_names,
        }

    # ========================================================
    # 3. EXPORT PARQUET
    # ========================================================
    @task
    def export_parquet(payload):
        table_name = payload["table_name"]
        col_names = payload["columns"]
        pg = PostgresHook(POSTGRES_CONN_ID)
        engine = pg.get_sqlalchemy_engine()

        file_path = (
            f"{TMP_DIR}/{table_name}.parquet"
        )

        query = (
            sql.SQL(
                "SELECT * FROM {}"
            )
            .format(
                sql.Identifier(table_name)
            )
            .as_string(
                pg.get_conn()
            )
        )

        import pyarrow as pa
        import pyarrow.parquet as pq

        writer = None

        try:
            for chunk in pd.read_sql(
                query,
                engine,
                chunksize=CHUNK_SIZE,
            ):
                chunk.columns = col_names
                table = pa.Table.from_pandas(
                    chunk,
                    preserve_index=False,
                )

                if writer is None:
                    writer = pq.ParquetWriter(
                        file_path,
                        table.schema,
                    )
                writer.write_table(table)

        finally:
            if writer is not None:
                writer.close()

        if writer is None:
            pd.DataFrame(
                columns=col_names
            ).to_parquet(
                file_path,
                index=False,
                engine="pyarrow",
            )

        return {
            "table": table_name,
            "file": file_path,
        }

    # ========================================================
    # 4. UPLOAD TO STAGE
    # ========================================================
    @task
    def upload(payload):
        sf = SnowflakeHook(
            SNOWFLAKE_CONN_ID
        )
        sf.run(
            f"""
            PUT file://{payload['file']}
            @{STAGE_NAME}
            AUTO_COMPRESS=FALSE
            OVERWRITE=TRUE
            """
        )
        return payload

    # ========================================================
    # 5. COPY INTO STAGING → SWAP
    # ========================================================
    @task
    def copy_into(payload):
        sf = SnowflakeHook(
            SNOWFLAKE_CONN_ID
        )
        table_name = payload["table"]
        staging_name = (
            f"{table_name}_staging"
        )

        sf.run(
            f"""
            COPY INTO
            {qualified_table(staging_name)}

            FROM
            @{STAGE_NAME}/{table_name}.parquet

            FILE_FORMAT = (
                TYPE = PARQUET
            )

            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE

            FORCE = TRUE
            """
        )

        sf.run(
            f"""
            ALTER TABLE
            {qualified_table(table_name)}

            SWAP WITH

            {qualified_table(staging_name)}
            """
        )

        return table_name

    # ========================================================
    # 6. VALIDATE
    #
    # PostgreSQL COUNT
    #       VS
    # Snowflake COUNT
    #
    # 실패해도 예외를 바로 발생시키지 않고
    # 결과를 monitoring task로 전달
    # ========================================================
    @task
    def validate(table_name):
        pg = PostgresHook(
            POSTGRES_CONN_ID
        )
        sf = SnowflakeHook(
            SNOWFLAKE_CONN_ID
        )

        # ----------------------------------------------------
        # PostgreSQL COUNT
        # ----------------------------------------------------
        pg_count = pg.get_first(
            sql.SQL(
                "SELECT COUNT(*) FROM {}"
            )
            .format(
                sql.Identifier(table_name)
            )
            .as_string(
                pg.get_conn()
            )
        )[0]

        # ----------------------------------------------------
        # Snowflake COUNT
        # ----------------------------------------------------
        sf_count = sf.get_first(
            f"""
            SELECT COUNT(*)
            FROM {qualified_table(table_name)}
            """
        )[0]

        # ----------------------------------------------------
        # 비교
        # ----------------------------------------------------
        count_diff = (
            pg_count - sf_count
        )

        if count_diff == 0:
            status = "SUCCESS"
            error_message = None

        else:
            status = "FAILED"
            error_message = (
                f"{table_name} mismatch: "
                f"PG={pg_count}, "
                f"SF={sf_count}"
            )

        # ----------------------------------------------------
        # Monitoring에 전달할 결과
        # ----------------------------------------------------
        return {
            "table_name": table_name,
            "pg_count": pg_count,
            "sf_count": sf_count,
            "count_diff": count_diff,
            "status": status,
            "error_message": error_message,
        }

    # ========================================================
    # 7. RECORD MONITORING
    #
    # validate 결과를
    # MIGRATION_MONITORING에 INSERT
    # ========================================================
    @task
    def record_monitoring(result):
        from airflow.operators.python import get_current_context
        context = get_current_context()

        # Airflow DAG Run ID
        run_id = context["dag_run"].run_id
        sf = SnowflakeHook(
            SNOWFLAKE_CONN_ID
        )

        sf.run(
            f"""
            INSERT INTO
            {qualified_table(MONITORING_TABLE)}
            (
                RUN_ID,
                PIPELINE_TYPE,
                TABLE_NAME,

                START_TIME,
                END_TIME,

                STATUS,

                PG_COUNT,
                SF_COUNT,
                COUNT_DIFF,

                ERROR_MESSAGE
            )

            VALUES
            (
                %(run_id)s,
                'FULL_LOAD',
                %(table_name)s,

                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP(),

                %(status)s,

                %(pg_count)s,
                %(sf_count)s,
                %(count_diff)s,

                %(error_message)s
            )
            """,

            parameters={

                "run_id": run_id,

                "table_name":
                    result["table_name"],

                "status":
                    result["status"],

                "pg_count":
                    result["pg_count"],

                "sf_count":
                    result["sf_count"],

                "count_diff":
                    result["count_diff"],

                "error_message":
                    result["error_message"],
            },
        )

        return result["table_name"]

    # ========================================================
    # 8. CLEANUP
    # ========================================================
    @task(
        trigger_rule="all_done"
    )
    def cleanup(table_name):
        sf = SnowflakeHook(
            SNOWFLAKE_CONN_ID
        )

        staging_name = (
            f"{table_name}_staging"
        )

        sf.run(
            f"""
            REMOVE
            @{STAGE_NAME}/{table_name}.parquet
            """
        )

        sf.run(
            f"""
            DROP TABLE IF EXISTS
            {qualified_table(staging_name)}
            """
        )

        local_file = (
            f"{TMP_DIR}/{table_name}.parquet"
        )

        if os.path.exists(local_file):
            os.remove(local_file)

        return (
            f"{table_name} cleaned"
        )

    # ========================================================
    # FLOW
    # ========================================================

    # --------------------------------------------------------
    # Monitoring table 준비
    # --------------------------------------------------------
    monitoring_ready = (
        ensure_monitoring_table()
    )
    tables = get_tables()
    monitoring_ready >> tables

    # --------------------------------------------------------
    # ETL Pipeline
    # --------------------------------------------------------
    with TaskGroup(
        "etl_pipeline"
    ):
        t1 = create_table.expand(table_name=tables)
        t2 = export_parquet.expand(payload=t1)
        t3 = upload.expand(payload=t2)
        t4 = copy_into.expand(payload=t3)
        t5 = validate.expand(table_name=t4)
        t6 = record_monitoring.expand(result=t5)
        cleanup.expand(table_name=t6)


# ============================================================
# DAG 생성
# ============================================================
full_load_monitoring()