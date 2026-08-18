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
    qualified_table,
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

# 새 소스를 추가할 때: sources/새소스.py 만들고 여기 딕셔너리에 한 줄만 추가
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
    return SOURCE_REGISTRY[source_type](conn_id=conn_id, schema=schema)


def load_sources() -> list[dict]:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)["sources"]


def build_full_load_dag(source_cfg: dict):
    """소스 설정 하나(dict)를 받아서 DAG 객체 하나를 생성해서 반환."""

    source_type = source_cfg["type"]
    conn_id = source_cfg["conn_id"]
    schema = source_cfg["schema"]
    sf_schema = source_cfg["snowflake_schema"]
    dag_id = f"full_load_{source_cfg['name']}"

    @dag(
        dag_id=dag_id,
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        max_active_tasks=5,
        default_args=DEFAULT_TASK_ARGS,
        tags=["full_load", source_type, source_cfg["name"]],
    )
    def _dag():

        # -----------------------------
        # 1. TABLE LIST
        # -----------------------------
        @task
        def get_tables():
            adapter = get_adapter(source_type, conn_id, schema)
            return adapter.get_tables()

        # -----------------------------
        # 2. CREATE TABLE (운영 + staging)
        # -----------------------------
        @task
        def create_table(table_name):
            adapter = get_adapter(source_type, conn_id, schema)
            sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

            cols = adapter.get_columns(table_name)
            if not cols:
                raise ValueError(f"No columns found for table {table_name}")

            ddl = ", ".join(
                f"{quote_identifier(col[0])} {adapter.map_type(col[1])}" for col in cols
            )
            staging_name = f"{table_name}_staging"

            create_tables(sf, sf_schema, table_name, staging_name, ddl)

            col_names = [c[0] for c in cols]
            return {"table_name": table_name, "columns": col_names}

        # -----------------------------
        # 3. EXPORT PARQUET (청크 단위)
        # -----------------------------
        @task
        def export_parquet(payload):
            adapter = get_adapter(source_type, conn_id, schema)

            table_name = payload["table_name"]
            col_names = payload["columns"]

            hook = adapter.get_hook()
            engine = hook.get_sqlalchemy_engine()
            query = adapter.build_select_query(table_name)

            file_path = f"{TMP_DIR}/{table_name}.parquet"
            export_to_parquet(engine, query, col_names, file_path, CHUNK_SIZE)

            return {"table": table_name, "file": file_path}

        # -----------------------------
        # 4. UPLOAD TO STAGE (소스 무관)
        # -----------------------------
        @task
        def upload(payload):
            sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
            upload_to_stage(sf, STAGE_NAME, payload["file"])
            return payload

        # -----------------------------
        # 5. COPY INTO STAGING -> SWAP (소스 무관)
        # -----------------------------
        @task
        def copy_into(payload):
            sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
            table_name = payload["table"]
            staging_name = f"{table_name}_staging"
            copy_into_and_swap(sf, sf_schema, STAGE_NAME, table_name, staging_name)
            return table_name

        # -----------------------------
        # 6. VALIDATE
        # -----------------------------
        @task
        def validate(table_name):
            adapter = get_adapter(source_type, conn_id, schema)
            sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

            src_count = adapter.get_row_count(table_name)
            sf_count = get_row_count(sf, sf_schema, table_name)

            if src_count != sf_count:
                raise ValueError(f"{table_name} mismatch: SRC={src_count}, SF={sf_count}")

            return table_name

        # -----------------------------
        # 7. CLEANUP (항상 실행, 소스 무관)
        # -----------------------------
        @task(trigger_rule="all_done")
        def cleanup(table_name):
            sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
            staging_name = f"{table_name}_staging"

            cleanup_stage_and_staging(sf, sf_schema, STAGE_NAME, table_name, staging_name)

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

    return _dag()


# -----------------------------
# YAML의 소스 개수만큼 DAG를 실제로 생성해서 전역 네임스페이스에 등록
# Airflow는 모듈의 전역 변수를 스캔해서 DAG 객체를 찾습니다.
# -----------------------------
for _source_cfg in load_sources():
    _dag_id = f"full_load_{_source_cfg['name']}"
    globals()[_dag_id] = build_full_load_dag(_source_cfg)