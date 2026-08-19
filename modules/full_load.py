import os
import yaml
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.task_group import TaskGroup

from sources.postgres import PostgresAdapter
from sources.mysql import MySQLAdapter
from sources.oracle import OracleAdapter

from common.parquet import export_to_parquet
from common.snowflake import (
    quote_identifier,
    create_tables,
    upload_to_stage,
    copy_into_and_swap,
    get_row_count,
    cleanup_stage_and_staging,
)


SNOWFLAKE_CONN_ID = "snowflake_conn"
STAGE_NAME = "MY_STAGE"
TMP_DIR = "/tmp"
CHUNK_SIZE = 50_000

DEFAULT_TASK_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

CONFIG_PATH = Path(__file__).parent / "config" / "sources.yaml"

SOURCE_REGISTRY = {
    "postgres": PostgresAdapter,
    "mysql": MySQLAdapter,
    "oracle": OracleAdapter,
}


def get_adapter(source_type: str, conn_id: str, schema: str):
    if source_type not in SOURCE_REGISTRY:
        raise ValueError(
            f"지원하지 않는 source_type: {source_type}. "
            f"사용 가능: {list(SOURCE_REGISTRY.keys())}"
        )

    return SOURCE_REGISTRY[source_type](
        conn_id=conn_id,
        schema=schema,
    )


def load_sources() -> list[dict]:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)["sources"]


@dag(
    dag_id="full_load",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=5,
    default_args=DEFAULT_TASK_ARGS,
    tags=["full_load"],
)
def full_load():
    # ============================================================
    # source별 ETL pipeline을 생성하는 함수
    # ============================================================
    def build_source_pipeline(source_cfg: dict):

        source_name = source_cfg["name"]
        source_type = source_cfg["type"]
        conn_id = source_cfg["conn_id"]
        schema = source_cfg["schema"]
        sf_schema = source_cfg["snowflake_schema"]

        # --------------------------------------------------------
        # 1. TABLE LIST
        # --------------------------------------------------------
        @task(task_id=f"{source_name}_get_tables")
        def get_tables():
            adapter = get_adapter(
                source_type,
                conn_id,
                schema,
            )

            return adapter.get_tables()

        # --------------------------------------------------------
        # 2. CREATE TABLE
        # --------------------------------------------------------
        @task(task_id=f"{source_name}_create_table")
        def create_table(table_name):

            adapter = get_adapter(
                source_type,
                conn_id,
                schema,
            )

            sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

            cols = adapter.get_columns(table_name)

            if not cols:
                raise ValueError(
                    f"No columns found for table {table_name}"
                )

            ddl = ", ".join(
                f"{quote_identifier(col[0])} "
                f"{adapter.map_type(col[1])}"
                for col in cols
            )

            # source가 달라도 동일한 table_name이 존재할 수 있으므로
            # Snowflake에서는 source_name을 포함하여 테이블 이름을 구분
            target_table_name = f"{source_name}_{table_name}"
            staging_name = f"{target_table_name}_staging"

            create_tables(
                sf,
                sf_schema,
                target_table_name,
                staging_name,
                ddl,
            )

            col_names = [c[0] for c in cols]

            return {
                "table_name": target_table_name,
                "source_table_name": table_name,
                "columns": col_names,
            }

        # --------------------------------------------------------
        # 3. EXPORT PARQUET
        # --------------------------------------------------------
        @task(task_id=f"{source_name}_export_parquet")
        def export_parquet(payload):

            adapter = get_adapter(
                source_type,
                conn_id,
                schema,
            )

            table_name = payload["table_name"]
            source_table_name = payload["source_table_name"]
            col_names = payload["columns"]

            hook = adapter.get_hook()
            engine = hook.get_sqlalchemy_engine()

            query = adapter.build_select_query(
                source_table_name
            )

            file_path = (
                f"{TMP_DIR}/"
                f"{source_name}_"
                f"{source_table_name}.parquet"
            )

            export_to_parquet(
                engine,
                query,
                col_names,
                file_path,
                CHUNK_SIZE,
            )

            return {
                "table": table_name,
                "source_table": source_table_name,
                "file": file_path,
            }

        # --------------------------------------------------------
        # 4. UPLOAD
        # --------------------------------------------------------
        @task(task_id=f"{source_name}_upload")
        def upload(payload):

            sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

            upload_to_stage(
                sf,
                STAGE_NAME,
                payload["file"],
            )

            return payload

        # --------------------------------------------------------
        # 5. COPY INTO + SWAP
        # --------------------------------------------------------
        @task(task_id=f"{source_name}_copy_into")
        def copy_into(payload):

            sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

            table_name = payload["table"]
            file_path = payload["file"]

            staging_name = f"{table_name}_staging"
            file_name = os.path.basename(file_path)

            copy_into_and_swap(
                sf,
                sf_schema,
                STAGE_NAME,
                table_name,
                staging_name,
                file_name,
            )

            return {
                "table": table_name,
                "source_table": payload["source_table"],
                "file": file_path,
                "file_name": file_name,
            }

        # --------------------------------------------------------
        # 6. VALIDATE
        # --------------------------------------------------------
        @task(task_id=f"{source_name}_validate")
        def validate(payload):

            adapter = get_adapter(
                source_type,
                conn_id,
                schema,
            )

            sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

            table_name = payload["table"]
            source_table_name = payload["source_table"]

            src_count = adapter.get_row_count(
                source_table_name
            )

            sf_count = get_row_count(
                sf,
                sf_schema,
                table_name,
            )

            if src_count != sf_count:
                raise ValueError(
                    f"{source_name}.{source_table_name} mismatch: "
                    f"SRC={src_count}, "
                    f"SF={sf_count}"
                )

            return payload

        # --------------------------------------------------------
        # 7. CLEANUP
        # --------------------------------------------------------
        @task(
            task_id=f"{source_name}_cleanup",
            trigger_rule="all_done",
        )
        def cleanup(payload):

            sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

            table_name = payload["table"]
            file_path = payload["file"]
            file_name = payload["file_name"]

            staging_name = f"{table_name}_staging"

            cleanup_stage_and_staging(
                sf,
                sf_schema,
                STAGE_NAME,
                table_name,
                staging_name,
                file_name,
            )

            if os.path.exists(file_path):
                os.remove(file_path)

            return f"{table_name} cleaned"

        # --------------------------------------------------------
        # FLOW
        # --------------------------------------------------------
        tables = get_tables()

        with TaskGroup(group_id=f"{source_name}_etl_pipeline"):
            t1 = create_table.expand(table_name=tables)
            t2 = export_parquet.expand(payload=t1)
            t3 = upload.expand(payload=t2)
            t4 = copy_into.expand(payload=t3)
            t5 = validate.expand(payload=t4)
            cleanup.expand(payload=t5)

    # ============================================================
    # YAML에 등록된 모든 source에 대해 pipeline 생성
    # ============================================================
    for source_cfg in load_sources():
        build_source_pipeline(source_cfg)


# ================================================================
# DAG 등록
# ================================================================
full_load()
