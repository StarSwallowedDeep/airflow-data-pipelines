from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd
import requests
import os

SNOWFLAKE_CONN_ID = "snowflake_conn"
SNOWFLAKE_SCHEMA = "KJH"
STAGE_NAME = "MY_STAGE"
FILE_FORMAT_NAME = "PARQUET_FORMAT"
TMP_DIR = "/tmp"
TABLE_NAME = "OPENMETEO_SEOUL_FORECAST"

# 회원가입/인증키 불필요한 Open-Meteo API
API_URL = "https://api.open-meteo.com/v1/forecast"

# 서울 좌표 (필요하면 다른 지역으로 바꿔서 확장 가능)
LATITUDE = 37.5665
LONGITUDE = 126.9780

# 가져올 일별 지표들. Open-Meteo 문서 보고 필요한 항목 추가/삭제 가능
DAILY_FIELDS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "precipitation_probability_max",
    "windspeed_10m_max",
]

DEFAULT_TASK_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def quote_sf_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def qualified_table(table_name: str) -> str:
    return f"{quote_sf_identifier(SNOWFLAKE_SCHEMA)}.{quote_sf_identifier(table_name)}"


def qualified_file_format() -> str:
    return f"{SNOWFLAKE_SCHEMA}.{FILE_FORMAT_NAME}"


@dag(
    dag_id="openmeteo_to_snowflake_daily",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",  # 매일 06:00 KST 근처 실행 (서버 타임존 설정에 맞게 조정 필요)
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_TASK_ARGS,
    tags=["openmeteo", "public_api", "daily_load"],
)
def openmeteo_to_snowflake_daily():

    # -----------------------------
    # 1. CREATE TABLE + FILE FORMAT (없으면 생성)
    # -----------------------------
    @task
    def create_table():
        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)

        # MERGE에서 스테이지 파일을 SELECT로 읽을 때 참조할 파일 포맷 (이름으로만 참조 가능하므로 미리 생성)
        sf.run(f"""
            CREATE FILE FORMAT IF NOT EXISTS {qualified_file_format()}
            TYPE = PARQUET
        """)

        # 따옴표 없이 생성 -> Snowflake가 대문자로 저장. FORECAST_DATE와 동일한 규칙으로 통일.
        cols_ddl = ", ".join(f"{f} FLOAT" for f in DAILY_FIELDS)
        sf.run(f"""
            CREATE TABLE IF NOT EXISTS {qualified_table(TABLE_NAME)} (
                FORECAST_DATE DATE,
                {cols_ddl},
                LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)

    # -----------------------------
    # 2. REQUEST (Open-Meteo API 호출)
    # -----------------------------
    @task
    def fetch_forecast():
        params = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "daily": ",".join(DAILY_FIELDS),
            "timezone": "Asia/Seoul",
            "forecast_days": 7,  # 향후 7일치. 필요에 따라 조정
        }
        resp = requests.get(API_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        daily = data.get("daily")
        if not daily:
            raise ValueError(f"Unexpected API response: {data}")

        return daily

    # -----------------------------
    # 3. PARSE + SAVE PARQUET
    # -----------------------------
    @task
    def parse_and_save(daily):
        df = pd.DataFrame({"forecast_date": daily["time"]})
        for field in DAILY_FIELDS:
            df[field] = daily.get(field)

        df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date

        file_path = f"{TMP_DIR}/{TABLE_NAME.lower()}.parquet"
        df.to_parquet(file_path, index=False, engine="pyarrow")

        return {
            "file": file_path,
            "row_count": len(df),
            "min_date": str(df["forecast_date"].min()),
            "max_date": str(df["forecast_date"].max()),
        }

    # -----------------------------
    # 4. UPLOAD TO STAGE
    # -----------------------------
    @task
    def upload(payload):
        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
        sf.run(
            f"PUT file://{payload['file']} @{STAGE_NAME} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        return payload

    # -----------------------------
    # 5. MERGE (같은 forecast_date가 있으면 갱신, 없으면 삽입)
    # -----------------------------
    @task
    def load(payload):
        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
        source_file = os.path.basename(payload["file"])

        target_cols = ["forecast_date"] + DAILY_FIELDS
        update_cols = DAILY_FIELDS  # forecast_date는 MATCH 키라서 UPDATE 대상에서 제외

        sf.run(f"""
            MERGE INTO {qualified_table(TABLE_NAME)} AS target
            USING (
                SELECT
                    $1:forecast_date::DATE AS forecast_date,
                    {", ".join(f"$1:{c}::FLOAT AS {c}" for c in DAILY_FIELDS)}
                FROM @{STAGE_NAME}/{source_file}
                (FILE_FORMAT => '{qualified_file_format()}')
            ) AS source
            ON target.FORECAST_DATE = source.forecast_date
            WHEN MATCHED THEN UPDATE SET
                {", ".join(f"{quote_sf_identifier(c.upper())} = source.{c}" for c in update_cols)},
                LOADED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                ({", ".join(quote_sf_identifier(c.upper()) for c in target_cols)})
                VALUES ({", ".join("source." + c for c in target_cols)})
        """)

        return payload

    # -----------------------------
    # 6. VALIDATE
    # -----------------------------
    @task
    def validate(payload):
        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
        today_count = sf.get_first(
            f"""
            SELECT COUNT(*) FROM {qualified_table(TABLE_NAME)}
            WHERE FORECAST_DATE >= CURRENT_DATE()
            """
        )[0]

        if today_count == 0:
            raise ValueError("오늘 이후 예보 데이터가 하나도 적재되지 않았습니다.")

        return payload

    # -----------------------------
    # 7. CLEANUP (항상 실행)
    #    스테이지 파일 / 로컬 파일은 매일 새로 생기는 일회성 데이터라 삭제.
    #    STAGE, FILE FORMAT은 재사용 인프라 객체라 여기서 지우지 않음.
    # -----------------------------
    @task(trigger_rule="all_done")
    def cleanup(payload):
        sf = SnowflakeHook(SNOWFLAKE_CONN_ID)
        source_file = os.path.basename(payload["file"])

        sf.run(f"REMOVE @{STAGE_NAME}/{source_file}")

        if os.path.exists(payload["file"]):
            os.remove(payload["file"])

        return "cleaned"

    # -----------------------------
    # FLOW
    # -----------------------------
    t0 = create_table()
    daily = fetch_forecast()
    t2 = parse_and_save(daily)
    t3 = upload(t2)
    t4 = load(t3)
    t5 = validate(t4)
    t6 = cleanup(t5)

    t0 >> daily


openmeteo_to_snowflake_daily()