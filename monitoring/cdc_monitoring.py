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
CDC_MONITORING_TABLE = "CDC_MONITORING"
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
    return '"' + name.replace('"', '""') + '"'


def qualified_table(table_name: str) -> str:
    return f"{quote_sf_identifier(SNOWFLAKE_SCHEMA)}.{quote_sf_identifier(table_name)}"


@dag(
    dag_id="cdc_monitoring",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=5,
    default_args=DEFAULT_TASK_ARGS,
    tags=["cdc", "monitoring", "postgres_to_snowflake"],
)
def cdc_monitoring():

    # ============================================================
    # 0. CONTROL / MONITORING TABLE 준비
    # ============================================================

    @task
    def ensure_control_tables():
        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

        sf.run(
            f"""
            CREATE TABLE IF NOT EXISTS {qualified_table(CDC_CONTROL_TABLE)}
            (
                TABLE_NAME STRING,
                UPDATED_AT_COL STRING,
                PK_COLUMNS STRING,
                DELETE_FLAG_COL STRING,
                LAST_WATERMARK TIMESTAMP_NTZ,
                LAST_RUN_AT TIMESTAMP_NTZ
            )
            """
        )

        sf.run(
            f"""
            CREATE TABLE IF NOT EXISTS {qualified_table(CDC_MONITORING_TABLE)}
            (
                DAG_RUN_ID STRING,
                TABLE_NAME STRING,
                CHECKED_AT TIMESTAMP_NTZ,
                STATUS STRING,
                SOURCE_ROW_COUNT NUMBER,
                TARGET_ROW_COUNT NUMBER,
                ROW_COUNT_DIFF NUMBER,
                CDC_ROW_COUNT NUMBER,
                INSERTED_ROW_COUNT NUMBER,
                UPDATED_ROW_COUNT NUMBER,
                DELETED_ROW_COUNT NUMBER,
                LAST_WATERMARK TIMESTAMP_NTZ,
                LAST_CDC_RUN_AT TIMESTAMP_NTZ
            )
            """
        )

        existing_columns = {
            r[0].lower()
            for r in sf.get_records(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %(schema)s
                  AND TABLE_NAME = %(table)s
                """,
                parameters={
                    "schema": SNOWFLAKE_SCHEMA,
                    "table": CDC_MONITORING_TABLE,
                },
            )
        }

        monitoring_columns = {
            "INSERTED_ROW_COUNT": "NUMBER",
            "UPDATED_ROW_COUNT": "NUMBER",
            "DELETED_ROW_COUNT": "NUMBER",
        }

        for column_name, column_type in monitoring_columns.items():
            if column_name.lower() not in existing_columns:
                sf.run(
                    f"""
                    ALTER TABLE {qualified_table(CDC_MONITORING_TABLE)}
                    ADD COLUMN {quote_sf_identifier(column_name)} {column_type}
                    """
                )

    # ============================================================
    # 1. CDC 대상 테이블 판별
    # ============================================================

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
                ORDER BY tablename
                """
            )
        ]

        eligible = []

        for table_name in tables:
            cols = pg.get_records(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %s
                ORDER BY ordinal_position
                """,
                parameters=(table_name,),
            )

            if not cols:
                continue

            col_names = [c[0] for c in cols]

            updated_at_col = next(
                (c for c in UPDATED_AT_CANDIDATES if c in col_names),
                None,
            )

            delete_flag_col = next(
                (c for c in DELETE_FLAG_CANDIDATES if c in col_names),
                None,
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

    # ============================================================
    # 2. 대상 테이블 준비 + 스키마 진화 + CONTROL 초기화
    # ============================================================

    @task
    def create_table(cfg):
        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

        table_name = cfg["table_name"]
        columns = cfg["columns"]
        column_types = cfg["column_types"]
        staging_name = f"{table_name}_cdc_staging"

        ddl = ", ".join(
            f"{quote_sf_identifier(col)} {map_type(column_types[col])}"
            for col in columns
        )

        exists = sf.get_first(
            """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %(schema)s
              AND TABLE_NAME = %(table)s
            """,
            parameters={
                "schema": SNOWFLAKE_SCHEMA,
                "table": table_name,
            },
        )[0]

        if not exists:
            sf.run(
                f"""
                CREATE TABLE IF NOT EXISTS
                {qualified_table(table_name)}
                ({ddl})
                """
            )
        else:
            existing_cols = {
                r[0].lower()
                for r in sf.get_records(
                    """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %(schema)s
                      AND TABLE_NAME = %(table)s
                    """,
                    parameters={
                        "schema": SNOWFLAKE_SCHEMA,
                        "table": table_name,
                    },
                )
            }

            for col in columns:
                if col.lower() not in existing_cols:
                    sf.run(
                        f"""
                        ALTER TABLE {qualified_table(table_name)}
                        ADD COLUMN {quote_sf_identifier(col)}
                        {map_type(column_types[col])}
                        """
                    )

        staging_exists = sf.get_first(
            """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %(schema)s
              AND TABLE_NAME = %(table)s
            """,
            parameters={
                "schema": SNOWFLAKE_SCHEMA,
                "table": staging_name,
            },
        )[0]

        if not staging_exists:
            sf.run(
                f"""
                CREATE TABLE IF NOT EXISTS
                {qualified_table(staging_name)}
                ({ddl})
                """
            )
        else:
            existing_staging_cols = {
                r[0].lower()
                for r in sf.get_records(
                    """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %(schema)s
                      AND TABLE_NAME = %(table)s
                    """,
                    parameters={
                        "schema": SNOWFLAKE_SCHEMA,
                        "table": staging_name,
                    },
                )
            }

            for col in columns:
                if col.lower() not in existing_staging_cols:
                    sf.run(
                        f"""
                        ALTER TABLE {qualified_table(staging_name)}
                        ADD COLUMN {quote_sf_identifier(col)}
                        {map_type(column_types[col])}
                        """
                    )

        sf.run(
            f"TRUNCATE TABLE {qualified_table(staging_name)}"
        )

        control_exists = sf.get_first(
            f"""
            SELECT COUNT(*)
            FROM {qualified_table(CDC_CONTROL_TABLE)}
            WHERE TABLE_NAME = %s
            """,
            parameters=(table_name,),
        )[0]

        if control_exists == 0:
            sf.run(
                f"""
                INSERT INTO {qualified_table(CDC_CONTROL_TABLE)}
                (
                    TABLE_NAME,
                    UPDATED_AT_COL,
                    PK_COLUMNS,
                    DELETE_FLAG_COL,
                    LAST_WATERMARK,
                    LAST_RUN_AT
                )
                VALUES
                (
                    %(table_name)s,
                    %(updated_at_col)s,
                    %(pk_cols)s,
                    %(delete_flag_col)s,
                    %(watermark)s,
                    NULL
                )
                """,
                parameters={
                    "table_name": table_name,
                    "updated_at_col": cfg["updated_at_col"],
                    "pk_cols": ",".join(cfg["pk_columns"]),
                    "delete_flag_col": cfg["delete_flag_col"] or "",
                    "watermark": DEFAULT_WATERMARK,
                },
            )

        return cfg

    # ============================================================
    # 3. 워터마크 이후 변경분 EXPORT
    # ============================================================

    @task
    def export_parquet(cfg):
        table_name = cfg["table_name"]
        col_names = cfg["columns"]
        updated_at_col = cfg["updated_at_col"]

        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

        watermark_row = sf.get_first(
            f"""
            SELECT LAST_WATERMARK
            FROM {qualified_table(CDC_CONTROL_TABLE)}
            WHERE TABLE_NAME = %s
            """,
            parameters=(table_name,),
        )

        watermark = (
            watermark_row[0]
            if watermark_row and watermark_row[0]
            else DEFAULT_WATERMARK
        )

        pg = PostgresHook(POSTGRES_CONN_ID)
        engine = pg.get_sqlalchemy_engine()
        conn = pg.get_conn()

        upper_watermark = pg.get_first(
            sql.SQL(
                """
                SELECT MAX({watermark_col})
                FROM {table}
                WHERE {watermark_col} > %(watermark)s
                """
            ).format(
                watermark_col=sql.Identifier(updated_at_col),
                table=sql.Identifier(table_name),
            ).as_string(conn),
            parameters={"watermark": watermark},
        )[0]

        file_path = f"{TMP_DIR}/{table_name}_cdc.parquet"

        if upper_watermark is None:
            return {
                **cfg,
                "file": file_path,
                "has_data": False,
                "row_count": 0,
                "new_watermark": str(watermark),
            }

        query = sql.SQL(
            """
            SELECT {cols}
            FROM {table}
            WHERE {watermark_col} > %(watermark)s
              AND {watermark_col} <= %(upper_watermark)s
            ORDER BY {watermark_col}
            """
        ).format(
            cols=sql.SQL(", ").join(
                sql.Identifier(c) for c in col_names
            ),
            table=sql.Identifier(table_name),
            watermark_col=sql.Identifier(updated_at_col),
        ).as_string(conn)

        import pyarrow as pa
        import pyarrow.parquet as pq

        writer = None
        row_count = 0

        try:
            for chunk in pd.read_sql(
                query,
                engine,
                params={
                    "watermark": watermark,
                    "upper_watermark": upper_watermark,
                },
                chunksize=CHUNK_SIZE,
            ):
                chunk.columns = col_names
                row_count += len(chunk)

                table = pa.Table.from_pandas(
                    chunk,
                    preserve_index=False,
                )

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
            "new_watermark": str(upper_watermark),
        }

    # ============================================================
    # 4. UPLOAD
    # ============================================================

    @task
    def upload(payload):
        if not payload["has_data"]:
            return payload

        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

        sf.run(
            f"""
            PUT file://{payload['file']}
            @{STAGE_NAME}
            AUTO_COMPRESS=FALSE
            OVERWRITE=TRUE
            """
        )

        return payload

    # ============================================================
    # 5. COPY INTO STAGING
    # ============================================================

    @task
    def copy_into_staging(payload):
        if not payload["has_data"]:
            return payload

        table_name = payload["table_name"]
        staging_name = f"{table_name}_cdc_staging"
        source_file = os.path.basename(payload["file"])

        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

        sf.run(
            f"TRUNCATE TABLE {qualified_table(staging_name)}"
        )

        sf.run(
            f"""
            COPY INTO {qualified_table(staging_name)}
            FROM @{STAGE_NAME}/{source_file}
            FILE_FORMAT = (
                TYPE = PARQUET
                USE_LOGICAL_TYPE = TRUE
            )
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE
            """
        )

        return payload

    # ============================================================
    # 6. VALIDATE
    # ============================================================

    @task
    def validate(payload):
        if not payload["has_data"]:
            return payload

        table_name = payload["table_name"]
        staging_name = f"{table_name}_cdc_staging"

        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

        staged_count = sf.get_first(
            f"""
            SELECT COUNT(*)
            FROM {qualified_table(staging_name)}
            """
        )[0]

        if staged_count != payload["row_count"]:
            raise ValueError(
                f"[PRE-MERGE VALIDATE FAIL] {table_name}: "
                f"extracted={payload['row_count']}, "
                f"staged={staged_count}"
            )

        return payload

    # ============================================================
    # 7. MERGE
    # ============================================================

    @task
    def merge_cdc(payload):
        table_name = payload["table_name"]

        if not payload["has_data"]:
            return {
                "table_name": table_name,
                "new_watermark": payload["new_watermark"],
                "cdc_row_count": 0,
                "inserted_row_count": 0,
                "updated_row_count": 0,
                "deleted_row_count": 0,
            }

        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

        staging_name = f"{table_name}_cdc_staging"
        pk_columns = payload["pk_columns"]
        columns = payload["columns"]
        delete_flag_col = payload.get("delete_flag_col")

        on_clause = " AND ".join(
            f"t.{quote_sf_identifier(pk)} = "
            f"s.{quote_sf_identifier(pk)}"
            for pk in pk_columns
        )

        if delete_flag_col:
            delete_condition = (
                f"s.{quote_sf_identifier(delete_flag_col)} = TRUE"
            )
            active_condition = f"NOT ({delete_condition})"
        else:
            delete_condition = "FALSE"
            active_condition = "TRUE"

        target_pk_null_condition = " AND ".join(
            f"t.{quote_sf_identifier(pk)} IS NULL"
            for pk in pk_columns
        )

        inserted_count = sf.get_first(
            f"""
            SELECT COUNT(*)
            FROM {qualified_table(staging_name)} s
            LEFT JOIN {qualified_table(table_name)} t
                ON {on_clause}
            WHERE {target_pk_null_condition}
              AND {active_condition}
            """
        )[0]

        updated_count = sf.get_first(
            f"""
            SELECT COUNT(*)
            FROM {qualified_table(staging_name)} s
            INNER JOIN {qualified_table(table_name)} t
                ON {on_clause}
            WHERE {active_condition}
            """
        )[0]

        deleted_count = sf.get_first(
            f"""
            SELECT COUNT(*)
            FROM {qualified_table(staging_name)} s
            INNER JOIN {qualified_table(table_name)} t
                ON {on_clause}
            WHERE {delete_condition}
            """
        )[0]

        update_columns = [
            c for c in columns
            if c not in pk_columns
        ]

        update_set = ", ".join(
            f"t.{quote_sf_identifier(c)} = "
            f"s.{quote_sf_identifier(c)}"
            for c in update_columns
        )

        insert_cols = ", ".join(
            quote_sf_identifier(c)
            for c in columns
        )

        insert_vals = ", ".join(
            f"s.{quote_sf_identifier(c)}"
            for c in columns
        )

        if delete_flag_col:
            merge_sql = f"""
                MERGE INTO {qualified_table(table_name)} t
                USING {qualified_table(staging_name)} s
                ON {on_clause}
                WHEN MATCHED
                    AND s.{quote_sf_identifier(delete_flag_col)} = TRUE
                    THEN DELETE
                WHEN MATCHED THEN
                    UPDATE SET {update_set}
                WHEN NOT MATCHED
                    AND (
                        s.{quote_sf_identifier(delete_flag_col)} IS NULL
                        OR s.{quote_sf_identifier(delete_flag_col)} = FALSE
                    )
                    THEN INSERT ({insert_cols})
                    VALUES ({insert_vals})
            """
        else:
            merge_sql = f"""
                MERGE INTO {qualified_table(table_name)} t
                USING {qualified_table(staging_name)} s
                ON {on_clause}
                WHEN MATCHED THEN
                    UPDATE SET {update_set}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_cols})
                    VALUES ({insert_vals})
            """

        sf.run(merge_sql)

        return {
            "table_name": table_name,
            "new_watermark": payload["new_watermark"],
            "cdc_row_count": payload["row_count"],
            "inserted_row_count": inserted_count,
            "updated_row_count": updated_count,
            "deleted_row_count": deleted_count,
        }

    # ============================================================
    # 8. 워터마크 갱신
    # ============================================================

    @task
    def update_watermark(result):
        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

        sf.run(
            f"""
            UPDATE {qualified_table(CDC_CONTROL_TABLE)}
            SET
                LAST_WATERMARK = %(watermark)s,
                LAST_RUN_AT = CURRENT_TIMESTAMP()
            WHERE TABLE_NAME = %(table_name)s
            """,
            parameters={
                "watermark": result["new_watermark"],
                "table_name": result["table_name"],
            },
        )

        return result

    # ============================================================
    # 9. CDC 모니터링 결과 저장
    # ============================================================

    @task
    def monitor(result):
        table_name = result["table_name"]

        pg = PostgresHook(POSTGRES_CONN_ID)
        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

        source_count = pg.get_first(
            f'SELECT COUNT(*) FROM public."{table_name}"'
        )[0]

        target_count = sf.get_first(
            f"""
            SELECT COUNT(*)
            FROM {qualified_table(table_name)}
            """
        )[0]

        row_count_diff = source_count - target_count

        control = sf.get_first(
            f"""
            SELECT
                LAST_WATERMARK,
                LAST_RUN_AT
            FROM {qualified_table(CDC_CONTROL_TABLE)}
            WHERE TABLE_NAME = %s
            """,
            parameters=(table_name,),
        )

        last_watermark = control[0] if control else None
        last_cdc_run_at = control[1] if control else None

        status = (
            "SUCCESS"
            if row_count_diff == 0
            else "MISMATCH"
        )

        sf.run(
            f"""
            INSERT INTO {qualified_table(CDC_MONITORING_TABLE)}
            (
                DAG_RUN_ID,
                TABLE_NAME,
                CHECKED_AT,
                STATUS,
                SOURCE_ROW_COUNT,
                TARGET_ROW_COUNT,
                ROW_COUNT_DIFF,
                CDC_ROW_COUNT,
                INSERTED_ROW_COUNT,
                UPDATED_ROW_COUNT,
                DELETED_ROW_COUNT,
                LAST_WATERMARK,
                LAST_CDC_RUN_AT
            )
            VALUES
            (
                %(dag_run_id)s,
                %(table_name)s,
                %(checked_at)s,
                %(status)s,
                %(source_row_count)s,
                %(target_row_count)s,
                %(row_count_diff)s,
                %(cdc_row_count)s,
                %(inserted_row_count)s,
                %(updated_row_count)s,
                %(deleted_row_count)s,
                %(last_watermark)s,
                %(last_cdc_run_at)s
            )
            """,
            parameters={
                "dag_run_id": "{{ run_id }}",
                "table_name": table_name,
                "checked_at": datetime.now(),
                "status": status,
                "source_row_count": source_count,
                "target_row_count": target_count,
                "row_count_diff": row_count_diff,
                "cdc_row_count": result["cdc_row_count"],
                "inserted_row_count": result["inserted_row_count"],
                "updated_row_count": result["updated_row_count"],
                "deleted_row_count": result["deleted_row_count"],
                "last_watermark": last_watermark,
                "last_cdc_run_at": last_cdc_run_at,
            },
        )

        return {
            "table_name": table_name,
            "status": status,
            "source_count": source_count,
            "target_count": target_count,
            "cdc_row_count": result["cdc_row_count"],
            "inserted_row_count": result["inserted_row_count"],
            "updated_row_count": result["updated_row_count"],
            "deleted_row_count": result["deleted_row_count"],
        }

    # ============================================================
    # 10. CLEANUP
    # ============================================================

    @task(trigger_rule="all_done")
    def cleanup(zipped):
        cfg, result = zipped
        table_name = cfg["table_name"]

        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
        staging_name = f"{table_name}_cdc_staging"

        sf.run(
            f"""
            REMOVE @{STAGE_NAME}/{table_name}_cdc.parquet
            """
        )

        sf.run(
            f"""
            DROP TABLE IF EXISTS
            {qualified_table(staging_name)}
            """
        )

        local_file = f"{TMP_DIR}/{table_name}_cdc.parquet"

        if os.path.exists(local_file):
            os.remove(local_file)

        return f"{table_name} cdc cleaned"

    # ============================================================
    # FLOW
    # ============================================================

    control_ready = ensure_control_tables()
    tables = get_cdc_tables()

    control_ready >> tables

    with TaskGroup("cdc_pipeline"):
        t1 = create_table.expand(cfg=tables)
        t2 = export_parquet.expand(cfg=t1)
        t3 = upload.expand(payload=t2)
        t4 = copy_into_staging.expand(payload=t3)
        t5 = validate.expand(payload=t4)
        t6 = merge_cdc.expand(payload=t5)
        t7 = update_watermark.expand(result=t6)
        t8 = monitor.expand(result=t7)
        cleanup_task = cleanup.expand(zipped=t1.zip(t7))


cdc_monitoring()