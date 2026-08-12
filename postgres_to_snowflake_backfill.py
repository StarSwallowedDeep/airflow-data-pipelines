from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

POSTGRES_CONN_ID = "postgres_db"
SNOWFLAKE_CONN_ID = "snowflake_conn"
SNOWFLAKE_SCHEMA = "KJH"
SNOWFLAKE_TABLE = "WATCH_EVENTS_DAILY"

DEFAULT_TASK_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id="postgres_to_snowflake_backfill",
    start_date=datetime(2026, 8, 10),
    schedule="0 1 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_TASK_ARGS,
    tags=["backfill", "postgres", "snowflake"],
)
def postgres_to_snowflake_backfill():

    # =========================================================
    # 1. 처리할 날짜/시간 구간 결정
    # =========================================================

    @task
    def get_interval(**context):

        logical_date = context["logical_date"]
        data_interval_start = context["data_interval_start"]
        data_interval_end = context["data_interval_end"]

        # 정상적인 Scheduled DAG Run이라면
        # data_interval_start < data_interval_end
        if data_interval_start < data_interval_end:
            start = data_interval_start
            end = data_interval_end

        # Airflow 3.x에서 backfill create를 했을 때
        # interval이 start == end로 생성되는 경우를 대비
        #
        # logical_date를 "하루 처리의 종료 시점"으로 사용한다.
        else:
            end = logical_date
            start = logical_date - timedelta(days=1)

        print("=" * 60)
        print("BACKFILL INTERVAL")
        print("=" * 60)

        print(f"logical_date       : {logical_date}")
        print(f"original start     : {data_interval_start}")
        print(f"original end       : {data_interval_end}")
        print("-" * 60)
        print(f"processing start   : {start}")
        print(f"processing end     : {end}")
        print("=" * 60)

        return {
            "start": start.isoformat(),
            "end": end.isoformat(),
        }

    # =========================================================
    # 2. POSTGRES에서 해당 날짜 데이터 조회
    # =========================================================

    @task
    def extract(interval):

        start = interval["start"]
        end = interval["end"]

        pg = PostgresHook(POSTGRES_CONN_ID)

        rows = pg.get_records(
            """
            SELECT
                event_id,
                user_id,
                content_id,
                watched_at,
                watch_minutes,
                device_type
            FROM public.watch_events
            WHERE watched_at >= %s
              AND watched_at < %s
            ORDER BY watched_at
            """,
            parameters=(start, end),
        )

        print("=" * 60)
        print("EXTRACT")
        print("=" * 60)

        print(f"interval start: {start}")
        print(f"interval end  : {end}")
        print(f"row count     : {len(rows)}")

        for row in rows[:10]:
            print(row)

        if len(rows) > 10:
            print(f"... {len(rows) - 10} more rows")

        print("=" * 60)

        return {
            "interval": interval,
            "rows": rows,
            "row_count": len(rows),
        }

    # =========================================================
    # 3. SNOWFLAKE TABLE 생성
    # =========================================================

    @task
    def create_table():

        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

        sf.run(
            f"""
            CREATE TABLE IF NOT EXISTS
            {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
            (
                EVENT_ID NUMBER,
                USER_ID NUMBER,
                CONTENT_ID NUMBER,
                WATCHED_AT TIMESTAMP_NTZ,
                WATCH_MINUTES NUMBER,
                DEVICE_TYPE VARCHAR
            )
            """
        )

        print(
            f"Table ready: "
            f"{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}"
        )

    # =========================================================
    # 4. 해당 날짜 데이터 적재
    # =========================================================

    @task
    def load(extracted):

        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

        interval = extracted["interval"]
        rows = extracted["rows"]

        start = interval["start"]
        end = interval["end"]

        table_name = (
            f"{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}"
        )

        # -----------------------------------------------------
        # 1. 해당 날짜 기존 데이터 삭제
        # -----------------------------------------------------

        sf.run(
            f"""
            DELETE FROM {table_name}
            WHERE WATCHED_AT >= %s
              AND WATCHED_AT < %s
            """,
            parameters=(start, end),
        )

        print("=" * 60)
        print("DELETE OLD DATA")
        print("=" * 60)

        print(f"interval: {start} ~ {end}")

        # -----------------------------------------------------
        # 2. 데이터가 없으면 종료
        # -----------------------------------------------------

        if not rows:

            print("No data for this interval.")

            return {
                "start": start,
                "end": end,
                "loaded_rows": 0,
            }

        # -----------------------------------------------------
        # 3. INSERT
        # -----------------------------------------------------

        sf.insert_rows(
            table=table_name,
            rows=rows,
            target_fields=[
                "EVENT_ID",
                "USER_ID",
                "CONTENT_ID",
                "WATCHED_AT",
                "WATCH_MINUTES",
                "DEVICE_TYPE",
            ],
        )

        print("=" * 60)
        print("LOAD")
        print("=" * 60)

        print(f"interval    : {start} ~ {end}")
        print(f"loaded rows : {len(rows)}")
        print("=" * 60)

        return {
            "start": start,
            "end": end,
            "loaded_rows": len(rows),
        }

    # =========================================================
    # 5. LOAD 결과 확인
    # =========================================================

    @task
    def validate(result):

        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

        start = result["start"]
        end = result["end"]

        count = sf.get_first(
            f"""
            SELECT COUNT(*)
            FROM {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
            WHERE WATCHED_AT >= %s
              AND WATCHED_AT < %s
            """,
            parameters=(start, end),
        )[0]

        print("=" * 60)
        print("VALIDATE")
        print("=" * 60)

        print(f"interval       : {start} ~ {end}")
        print(f"expected rows  : {result['loaded_rows']}")
        print(f"actual rows    : {count}")

        if count != result["loaded_rows"]:
            raise ValueError(
                f"Validation failed: "
                f"expected={result['loaded_rows']}, "
                f"actual={count}"
            )

        print("Validation SUCCESS")
        print("=" * 60)

        return count

    # =========================================================
    # FLOW
    # =========================================================

    interval = get_interval()
    extracted = extract(interval)
    table = create_table()
    loaded = load(extracted)

    table >> loaded

    validated = validate(loaded)


postgres_to_snowflake_backfill()