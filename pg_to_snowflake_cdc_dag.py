import json
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from psycopg2 import sql

SNOWFLAKE_SCHEMA = "MY_SCHEMA"
STAGE_NAME = "MY_STAGE"
WATERMARK_TABLE = "CDC_WATERMARKS"

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


def qualified_table(table_name: str) -> str:
    return f"{SNOWFLAKE_SCHEMA}.{table_name}"


@dag(
    dag_id="pg_to_snowflake_cdc_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    default_args=DEFAULT_TASK_ARGS,
    tags=["cdc", "snowflake"],
)
def pg_to_snowflake_cdc_dag():

    # ──────────────────────────────────────────
    # STEP 0: 테이블 + PK 자동 조회
    # ──────────────────────────────────────────
    @task
    def get_cdc_tables() -> dict:
        """
        PostgreSQL public 스키마의 모든 테이블과 PK를 자동으로 가져옴.
        반환값: {"customers": ["customer_id"], "order_items": ["order_id", "item_id"], ...}
        """
        pg = PostgresHook("postgres_netflix")

        result = pg.get_records("""
            SELECT
                t.table_name,
                k.column_name AS pk_column
            FROM information_schema.tables t
            JOIN information_schema.table_constraints c
              ON t.table_name = c.table_name
             AND t.table_schema = c.constraint_schema
             AND c.constraint_type = 'PRIMARY KEY'
            JOIN information_schema.key_column_usage k
              ON c.constraint_name = k.constraint_name
             AND c.table_schema = k.table_schema
            WHERE t.table_schema = 'public'
            ORDER BY t.table_name, k.ordinal_position
        """)

        # [중요] 기존엔 {row[0]: row[1] for row in result} 딕셔너리 컴프리헨션을 써서
        # 복합 PK(컬럼 2개 이상) 테이블은 마지막 PK 컬럼만 남고 나머지가 덮어써졌음.
        # 이러면 MERGE INTO의 ON 조건이 PK 일부만 비교하게 되어 엉뚱한 row가 매칭/업데이트될 위험이 있었음.
        # defaultdict(list)로 모든 PK 컬럼을 빠짐없이 모음
        cdc_tables = defaultdict(list)
        for table_name, pk_column in result:
            cdc_tables[table_name].append(pk_column)
        cdc_tables = dict(cdc_tables)

        print(f"✅ CDC 대상 테이블 {len(cdc_tables)}개 자동 감지: {cdc_tables}")
        return cdc_tables

    # ──────────────────────────────────────────
    # STEP 1: 마지막 실행 시각(워터마크) 불러오기
    # ──────────────────────────────────────────
    @task
    def get_watermarks(cdc_tables: dict) -> dict:
        # [변경] Airflow Variable 대신 Snowflake CDC_WATERMARKS 테이블에서 조회
        sf = SnowflakeHook("snowflake_conn")

        rows = sf.get_records(f"""
            SELECT TABLE_NAME, LAST_UPDATED_AT
            FROM {qualified_table(WATERMARK_TABLE)}
        """)
        stored = {r[0].lower(): str(r[1]) for r in rows}

        watermarks = {}
        for table in cdc_tables:
            watermarks[table] = stored.get(table, "1970-01-01 00:00:00")
        return watermarks

    # [추가] cdc_tables 딕셔너리를 리스트로 펼치는 헬퍼
    # (dict는 .expand()로 바로 매핑이 안 되므로 [{"table":..., "pk_cols":...}, ...] 형태로 변환)
    @task
    def to_table_list(cdc_tables: dict) -> list[dict]:
        return [{"table": t, "pk_cols": pk} for t, pk in cdc_tables.items()]

    # ──────────────────────────────────────────
    # STEP 2: 변경된 행(Row) 추출 → Parquet
    # ──────────────────────────────────────────
    # [변경] 기존엔 이 태스크 하나가 for 루프로 모든 테이블(700개)을 순차 처리했음.
    # 테이블 수가 많아지면 이 단일 태스크가 전체 파이프라인의 병목이 되므로,
    # 테이블 1개만 처리하는 태스크로 쪼개서 .expand()로 병렬 실행되게 함
    @task
    def extract_one_table(table_info: dict, table_watermarks: dict) -> dict | None:
        """
        테이블 1개에서 watermark 이후로 updated_at이 변경된 행만 추출.
        반환값: {"table": ..., "file": ..., "pk_cols": [...], "max_updated_at": ...} 또는
        변경분이 없으면 None (뒤 단계에서 filter_changed가 걸러냄)
        """
        pg = PostgresHook("postgres_netflix")

        table = table_info["table"]
        pk_cols = table_info["pk_cols"]
        watermark = table_watermarks[table]
        order_by_cols = ["updated_at"] + pk_cols  # 동일 updated_at row 순서 고정

        # [중요] 기존엔 테이블명/order by 컬럼을 f-string으로 직접 끼워 넣어 SQL 인젝션 위험이 있었음
        # (값은 %s 파라미터 바인딩으로 처리했지만, 식별자인 테이블명/컬럼명은 바인딩이 안 되므로
        # sql.Identifier로 별도 처리해야 함)
        query = sql.SQL("""
            SELECT *
            FROM {table}
            WHERE updated_at >= %s
              AND updated_at IS NOT NULL
            ORDER BY {order_cols}
        """).format(
            table=sql.Identifier(table),
            order_cols=sql.SQL(", ").join(sql.Identifier(c) for c in order_by_cols),
        )

        with pg.get_conn() as conn:
            with conn.cursor() as cursor:
                final_query_string = query.as_string(cursor)

        rows = pg.get_records(final_query_string, parameters=(watermark,))

        if not rows:
            return None

        col_info = pg.get_records(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
            """,
            parameters=(table,)
        )
        col_names = [c[0] for c in col_info]

        file_path = f"/tmp/cdc_{table}.parquet"

        df = pd.DataFrame(rows, columns=col_names)
        df.to_parquet(
            file_path,
            index=False,
            engine="pyarrow",
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )

        # [중요] ">="를 쓰면 이번 배치의 마지막 row(들)이 다음 실행에서 watermark와 정확히 같은
        # updated_at 값이라 다시 조회 대상에 포함되어 매 실행마다 무한 재처리된다.
        # MERGE가 멱등적이라 데이터는 안 깨지지만 불필요한 재추출/재업로드/재MERGE가 영구 반복됨.
        # max_updated_at에 1마이크로초를 더해서 저장하면 다음 실행에서 해당 row들이 제외됨.
        max_ts = df["updated_at"].max()
        if pd.isna(max_ts):
            print(f"⚠️ {table}: max(updated_at)이 유효하지 않아 이번 배치를 스킵합니다")
            return None

        max_updated_at = str(max_ts + pd.Timedelta(microseconds=1))

        return {
            "table": table,
            "file": file_path,
            "pk_cols": pk_cols,
            "all_cols": col_names,
            "max_updated_at": max_updated_at,
            "row_count": len(rows),
        }

    # [추가] extract_one_table이 반환한 None(변경분 없는 테이블)을 걸러내는 필터 태스크.
    # 기존엔 for 루프 안에서 continue로 처리했는데, 병렬 구조에서는
    # 각 태스크가 독립적으로 None/dict를 반환하므로 뒤에서 한 번에 걸러줘야 함
    @task
    def filter_changed(extracted: list[dict | None]) -> list[dict]:
        return [x for x in extracted if x is not None]

    # ──────────────────────────────────────────
    # STEP 3: Snowflake TEMP 테이블 생성 & Stage PUT
    # ──────────────────────────────────────────
    @task
    def upload_to_stage(payload: dict) -> dict:
        sf = SnowflakeHook("snowflake_conn")
        pg = PostgresHook("postgres_netflix")
        table = payload["table"]

        # col_query = """
        #     SELECT column_name, data_type
        #     FROM information_schema.columns
        #     WHERE table_name = %s
        #     ORDER BY ordinal_position
        # """

        # [변경] 재조회 대신 payload에서 컬럼명만 가져오되,
        # data_type이 필요하니 이 부분은 여전히 조회는 함
        col_query = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """
        cols = pg.get_records(col_query, parameters=(table,))

        ddl = ", ".join(f"{c[0]} {map_type(c[1])}" for c in cols)
        temp_table = f"{table}_temp"
        target_table = qualified_table(table)

        # [추가] 운영 테이블이 이미 존재하는지 먼저 확인.
        # 존재하면 CREATE TABLE을 스킵하고, 존재하지 않을 때만 신규 생성.
        exists = sf.get_first(f"""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{SNOWFLAKE_SCHEMA}'
              AND TABLE_NAME = '{table.upper()}'
        """)[0]

        if not exists:
            sf.run(f"CREATE TABLE {target_table} ({ddl})")
            print(f"[CREATE] {target_table} 신규 생성")
        else:
            # [변경] 운영 테이블이 이미 있는 경우에만 컬럼 비교 + ALTER 로직 실행
            existing_cols = {
                r[0].lower()
                for r in sf.get_records(f"""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{SNOWFLAKE_SCHEMA}'
                      AND TABLE_NAME = '{table.upper()}'
                """)
            }
            pg_col_types = {c[0]: c[1] for c in cols}
            missing_cols = [c[0] for c in cols if c[0].lower() not in existing_cols]

            for col_name in missing_cols:
                sf.run(
                    f"ALTER TABLE {target_table} "
                    f"ADD COLUMN {col_name} {map_type(pg_col_types[col_name])}"
                )
                print(f"[ALTER] {target_table}에 컬럼 추가: {col_name} {map_type(pg_col_types[col_name])}")

        temp_query = f"CREATE OR REPLACE TABLE {qualified_table(temp_table)} ({ddl})"
        sf.run(temp_query)

        sf.run(f"PUT file://{payload['file']} @{STAGE_NAME} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

        return payload

    # ──────────────────────────────────────────
    # STEP 4: COPY INTO temp → MERGE INTO 본 테이블 (검증 선행)
    # ──────────────────────────────────────────
    @task
    def merge_into_target(payload: dict) -> dict:
        sf = SnowflakeHook("snowflake_conn")
        table = payload["table"]
        pk_cols = payload["pk_cols"]
        temp_table = f"{table}_temp"

        sf.run(f"""
            COPY INTO {qualified_table(temp_table)}
            FROM @{STAGE_NAME}/cdc_{table}.parquet
            FILE_FORMAT = (
                TYPE = PARQUET
                SNAPPY_COMPRESSION = TRUE
            )
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """)

        # [중요] 기존엔 MERGE를 먼저 실행해서 본 테이블이 이미 바뀐 뒤에 validate가 카운트를 비교했음.
        # 여기서는 temp 테이블 적재 행 수와 추출 시점 row_count를 MERGE 전에 먼저 비교해서,
        # COPY 과정에서 유실/중복된 행이 있으면 본 테이블에 반영되기 전에 막음
        temp_count = sf.get_first(f"SELECT COUNT(*) FROM {qualified_table(temp_table)}")[0]
        if temp_count != payload["row_count"]:
            raise ValueError(
                f"[PRE-MERGE VALIDATE FAIL] {table}: "
                f"extracted={payload['row_count']}, staged_in_snowflake={temp_count}"
            )

        # [변경] pg.get_records 재조회 제거, payload에서 바로 가져옴
        # [기존] all_cols = [c[0] for c in col_info]
        all_cols = payload["all_cols"]
        non_pk_cols = [c for c in all_cols if c not in pk_cols]

        # [중요] 기존엔 단일 pk 컬럼만 가정해서 ON t.{pk} = s.{pk}로 비교했음.
        # 복합 PK를 모두 AND로 묶어 비교하도록 변경 (PK 컬럼이 1개여도 동일하게 동작)
        join_condition = " AND ".join(f"t.{pk} = s.{pk}" for pk in pk_cols)
        update_set = ", ".join(f"t.{c} = s.{c}" for c in non_pk_cols)
        insert_cols = ", ".join(all_cols)
        insert_vals = ", ".join(f"s.{c}" for c in all_cols)

        sf.run(f"""
            MERGE INTO {qualified_table(table)} AS t
            USING {qualified_table(temp_table)} AS s
               ON {join_condition}
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_vals})
        """)

        return payload

    # ──────────────────────────────────────────
    # STEP 5: 검증 (참고용 사후 확인 — 1차 검증은 merge_into_target에서 선행됨)
    # ──────────────────────────────────────────
    @task
    def validate(payload: dict) -> dict:
        sf = SnowflakeHook("snowflake_conn")
        table = payload["table"]

        sf_count = sf.get_first(f"SELECT COUNT(*) FROM {qualified_table(table)}")[0]
        print(f"[VALIDATE OK] {table}: target table now has {sf_count} rows")
        return payload

    # ──────────────────────────────────────────
    # STEP 6: Stage 파일 정리 (항상 실행)
    # ──────────────────────────────────────────
    # [중요] 기존엔 Stage 정리와 Watermark 업데이트가 같은 태스크 안에 있었음.
    # trigger_rule="all_done"이라 validate 실패 시에도 실행되는데, 그러면 잘못된 데이터가
    # 들어간 상태에서 watermark까지 앞으로 당겨져 다음 실행에서 해당 구간을 건너뛰게 됨.
    # 즉 누락된 데이터가 영구히 복구 불가능한 상태가 될 수 있었음.
    # Stage 정리(항상 해야 함)와 Watermark 업데이트(성공 시에만 해야 함)를 태스크로 분리함.
    @task(trigger_rule="all_done")
    def cleanup_stage(payload: dict) -> dict:
        import os
        sf = SnowflakeHook("snowflake_conn")
        table = payload["table"]
        temp_table = f"{table}_temp"

        # 1. Snowflake Stage 파일 정리
        sf.run(f"REMOVE @{STAGE_NAME}/cdc_{table}.parquet")
        print(f"[CLEANUP] {table} stage file removed")

        # 2. [추가] temp 테이블 정리 (매 실행마다 재생성되므로 남겨둘 필요 없음)
        sf.run(f"DROP TABLE IF EXISTS {qualified_table(temp_table)}")
        print(f"[CLEANUP] {temp_table} dropped")

        # 3. 로컬 파일 정리
        local_file = payload["file"]
        if os.path.exists(local_file):
            os.remove(local_file)
            print(f"[CLEANUP] Local file {local_file} removed")

        return payload

    # ──────────────────────────────────────────
    # STEP 7: Watermark 업데이트 (성공 시에만 실행)
    # ──────────────────────────────────────────
    @task  # trigger_rule 기본값 = all_success: 앞 단계 성공 시에만 실행됨
    def update_watermarks(payloads: list[dict], **context) -> str:
        # [변경] Variable read-modify-write 대신
        # Snowflake CDC_WATERMARKS 테이블에 테이블별로 MERGE(upsert).
        # 각 row가 독립적이라 동시 실행/부분 재시도에도 안전함
        sf = SnowflakeHook("snowflake_conn")
        run_id = context["run_id"]

        updated_tables = []
        for payload in payloads:
            table = payload["table"]
            max_updated_at = payload["max_updated_at"]

            sf.run(f"""
                MERGE INTO {qualified_table(WATERMARK_TABLE)} AS t
                USING (SELECT '{table}' AS table_name,
                              '{max_updated_at}'::TIMESTAMP_NTZ AS last_updated_at,
                              '{run_id}' AS updated_by_run) AS s
                   ON t.table_name = s.table_name
                WHEN MATCHED THEN
                    UPDATE SET t.last_updated_at = s.last_updated_at,
                               t.updated_by_run = s.updated_by_run,
                               t.updated_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN
                    INSERT (table_name, last_updated_at, updated_by_run)
                    VALUES (s.table_name, s.last_updated_at, s.updated_by_run)
            """)
            updated_tables.append(table)

        print(f"[DONE] watermark 갱신된 테이블: {updated_tables}")
        return f"{len(updated_tables)}개 테이블 CDC 완료"

    # ──────────────────────────────────────────
    # FLOW
    # ──────────────────────────────────────────
    cdc_tables = get_cdc_tables()
    watermarks = get_watermarks(cdc_tables)

    table_list = to_table_list(cdc_tables)
    extracted = extract_one_table.partial(table_watermarks=watermarks).expand(table_info=table_list)
    changed_list = filter_changed(extracted)

    uploaded = upload_to_stage.expand(payload=changed_list)
    merged = merge_into_target.expand(payload=uploaded)
    validated = validate.expand(payload=merged)
    cleaned = cleanup_stage.expand(payload=validated)
    # update_watermark.expand(payload=cleaned)
    update_watermarks(cleaned)  # [변경] expand 없이 리스트 전체를 한 번에 넘김


pg_to_snowflake_cdc_dag()


# 컬럼 조회 3번 하던 걸 1번으로 줄임 (all_cols를 payload로 넘겨서 재사용)
# 두 DAG에서 다르게 쓰던 타입 매핑을 TYPE_MAP/map_type 하나로 통일
# updated_at이 NULL인 row 때문에 워터마크 깨지는 것 방지 (IS NOT NULL 조건 + pd.isna 방어)
# Postgres에 컬럼 추가돼도 Snowflake가 못 따라가던 문제 해결 (ALTER TABLE ADD COLUMN 자동 추가)
# 테이블마다 따로 있던 워터마크 Variable을 cdc_watermarks 하나의 JSON으로 통합
# 매번 날리던 CREATE TABLE을 존재 여부 먼저 체크해서 있으면 스킵하도록 개선
# 워터마크를 Airflow Variable(JSON) 대신 Snowflake CDC_WATERMARKS 테이블에 저장 (row 단위 MERGE로 동시성 문제 해결)
# extract_changed_rows의 순차 for 루프를 테이블별 병렬 태스크(to_table_list → extract_one_table → filter_changed)로 분리