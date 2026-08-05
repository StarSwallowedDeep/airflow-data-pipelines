# Airflow 실습 정리: PostgreSQL → Snowflake Full Load / CDC Pipeline

## 개요

PostgreSQL 데이터를 Snowflake로 이관하는 데이터 파이프라인을 구축하면서 Airflow의 핵심 기능인 Dynamic Task Mapping, DAG Run, Trigger, Sensor, Logical Date 개념을 실습했다.

구현한 파이프라인은 크게 두 가지이다.

**Full Load DAG**
- 초기 데이터 전체 이관
- PostgreSQL의 전체 테이블 데이터를 Snowflake로 적재

**CDC DAG**
- Full Load 이후 변경 데이터만 추적하여 Snowflake에 반영
- 운영 환경에서 지속적인 데이터 동기화를 담당

본 문서는 다음 내용을 정리한다.
- Full Load / CDC 설계 배경
- DAG 구조 및 구현 시 고려사항
- Airflow 핵심 개념 실습(Logical Date, Trigger, Sensor)

---

## Part 1. Full Load / CDC 개념 및 설계 배경

### 1.1 Full Load란?

Full Load는 원본 시스템의 데이터를 전체 조회하여 대상 시스템에 처음부터 적재하는 방식이다.

예:

```
PostgreSQL:
customer table
10,000,000 rows

Snowflake 초기 구축 시:
customer 전체 데이터
        ↓
Snowflake customer table
```

형태로 모든 데이터를 가져온다.

**특징**

| 항목 | 내용 |
|---|---|
| 목적 | 초기 데이터 구축 |
| 처리 대상 | 전체 데이터 |
| 실행 빈도 | 최초 1회 또는 필요 시 |
| 장점 | 전체 데이터 기준 검증 가능 |
| 단점 | 데이터량 증가 시 시간과 비용 증가 |

Full Load는 신규 Data Warehouse 구축이나 시스템 마이그레이션 과정에서 주로 사용한다.

### 1.2 CDC(Change Data Capture)란?

CDC는 이미 적재된 데이터 이후 발생한 변경 사항만 추적하여 대상 시스템에 반영하는 방식이다.

예:

```
Full Load 완료 시점:
10:00
PostgreSQL = Snowflake

이후 발생:
INSERT customer A
UPDATE customer B
DELETE customer C
```

변경 데이터만 추출하여 Snowflake에 반영한다.

**특징**

| 항목 | 내용 |
|---|---|
| 목적 | 운영 데이터 동기화 |
| 처리 대상 | 변경 데이터 |
| 실행 빈도 | 주기적 실행 |
| 장점 | 전체 데이터를 다시 읽지 않음 |
| 필요 요소 | 변경 지점 관리(Watermark 등) |

### 1.3 Full Load와 CDC를 별도 DAG으로 분리한 이유

Full Load와 CDC는 목적과 실행 방식이 다르기 때문에 하나의 DAG에서 처리하지 않고 분리했다.

**Full Load DAG**
- 목적: 초기 데이터 구축
- 특징:
  - 전체 데이터 처리
  - 대량 데이터 처리 필요
  - 실행 빈도 낮음
  - 실패 시 전체 재처리 가능

**CDC DAG**
- 목적: 운영 데이터 지속 동기화
- 특징:
  - 변경 데이터만 처리
  - 짧은 주기로 반복 실행
  - 데이터 누락 방지가 중요
  - 상태 관리 필요

따라서 다음 구조로 설계했다.

```
## 초기 구축
Full Load DAG
       |
       ▼
## 지속 동기화
CDC DAG
(schedule 실행)
```

DAG을 분리함으로써:
- 실행 주기 독립 관리
- 장애 영향 범위 분리
- Full Load 완료 후 CDC 시작 제어
- 운영 환경에서 안정적인 데이터 동기화

가 가능하도록 구성했다.

---

## Part 2. 코드 구조 및 설계 시 고려사항

### 2.1 공통 설계 원칙

일반적으로 테이블 하나하나를 코드에 정의하는 대신, 대상 DB(Postgres)에 있는 모든 테이블 정보를 동적으로 조회하고, Dynamic Task Mapping으로 테이블 하나당 하나의 태스크 흐름이 생성되도록 설계했다. 테이블이 700개 규모까지 늘어나도 코드 수정 없이 대응 가능하다는 게 핵심 목표.

```
[get_tables()] → 테이블 목록 반환
       │
       ▼
[create_table.expand(table_name=tables)]   ← 테이블 개수만큼 자동으로 태스크 생성
       │
       ▼
   ... (이하 각 테이블별로 독립적인 파이프라인 진행)
```

### 2.2 Full Load DAG 설계 특징

| 항목 | 설계 내용 |
|---|---|
| 파이프라인 단계 | 6단계로 완전 분리 (create_table → export_parquet → upload → copy_into → validate → cleanup) |
| 태스크 구조 | Dynamic Task Mapping을 사용하여 테이블별 하나의 Task Instance가 생성되는 구조로 설계 (예: 700개 테이블이면 각 단계별 Task가 700개씩 생성) |
| 테이블별 분리 이유 | 모든 테이블을 하나의 Task에서 순차 처리하지 않고 테이블 단위로 분리하여, 특정 테이블 실패가 전체 Full Load 처리에 영향을 주지 않도록 구성하고 병렬 처리 효율을 높임 |
| 적재 방식 | staging 테이블에 먼저 COPY INTO → 검증 → SWAP로 운영 테이블 교체 |
| 중간 실패 시 안전성 | COPY 실패해도 운영 테이블은 그대로 유지 (staging만 날아감) — 원자적 교체 |
| 데이터 읽기 방식 | chunksize=50_000으로 나눠 읽고 Parquet에 이어쓰기 (메모리 절약) |
| 검증(validate) | Postgres 건수 vs Snowflake 건수 비교, 불일치 시 에러 발생시켜 파이프라인 중단 |
| 정리(cleanup) 실행 조건 | trigger_rule="all_done"인 별도 태스크로, 실패 여부와 무관하게 Stage 파일 + 로컬 파일 + staging 테이블 항상 정리 |
| 재시도(retry) | default_args로 전체 태스크에 재시도 2회 + 2분 대기 적용 |
| 동시 실행 제한 | max_active_tasks=5로 동시 실행 개수 제한 (Snowflake/Postgres 부하 조절) |
| 스키마 지정 | SNOWFLAKE_SCHEMA 변수로 명시 지정, qualified_table() 헬퍼로 항상 스키마.테이블 형태 사용 (SQL 인젝션 방어 포함) |

### 2.3 CDC DAG 설계 특징

| 항목 | 설계 내용 |
|---|---|
| 태스크 구조 | 테이블 탐색 → watermark 조회 → 변경분 추출 → Stage 업로드 → 검증 → MERGE → watermark 갱신 |
| Watermark 관리 | Airflow Variable이 아닌 Snowflake CDC_CONTROL 테이블에서 테이블별 마지막 처리 시각 관리 |
| 변경 데이터 추출 방식 | updated_at류 컬럼 기반 watermark 조건으로 이전 처리 이후 변경분만 추출 |
| 적재 방식 | 변경분 → Parquet 변환 → Snowflake Stage → staging 테이블 COPY INTO → Target Table MERGE |
| MERGE 처리 | Primary Key 기반 UPSERT, 복합 PK 테이블도 지원 |
| Dynamic Task Mapping | 테이블별 CDC 작업을 병렬 Task로 분리해 대량 테이블 처리 |
| Schema Evolution | Postgres에 신규 컬럼 생기면 Snowflake ALTER TABLE ADD COLUMN 자동 반영 |
| 검증(validate) | Stage COPY 이후 적재 건수 비교로 데이터 유실 여부 확인한 뒤에만 MERGE 수행 (게이트 역할) |
| Watermark 갱신 안정성 | MERGE + 검증 성공 이후에만 watermark 갱신 → 실패 시 재처리 가능하도록 설계 |
| 정리(cleanup) | trigger_rule="all_done"인 별도 태스크로 Stage 파일, staging 테이블, 로컬 Parquet 파일 정리 |
| 재시도(retry) | default_args로 실패 태스크 자동 재시도 (2회 + 2분 대기) |
| 동시 실행 제한 | max_active_tasks로 병렬 실행 개수 제어, max_active_runs=1로 이전 실행 미완료 시 다음 스케줄 실행 방지(레이스 컨디션 방지) |
| DELETE 처리 | updated_at 기반 CDC 방식에서는 물리 DELETE 감지 불가 → soft delete 컬럼 또는 별도 delete log 필요 |
| Staging 테이블 최적화 | CREATE OR REPLACE 대신 CREATE TABLE IF NOT EXISTS + TRUNCATE TABLE 사용 → 불필요한 DDL 실행 감소, staging 테이블 재사용 |
| Cleanup 신뢰성 | config와 merge 결과를 인덱스 기준 1:1 매칭 → cleanup이 "전체 태스크 완료"가 아닌 "자기 테이블의 처리 결과"만 기다리도록 설계 (700개 테이블 중 하나가 느려도 나머지 cleanup이 블로킹되지 않음) |

### 2.4 데이터 흐름 아키텍처 (Full Load 기준)

태스크 단위가 아니라, 데이터(파일)가 물리적으로 어디에 머무는지 기준으로 보면 아래 순서로 흘러간다. CDC도 파일명 규칙(`_cdc.parquet`)과 대상 테이블(staging → MERGE)만 다를 뿐 동일한 흐름을 탄다.

```
┌───────────────┐   1. SELECT (chunksize)    ┌──────────────────────────┐
│   Postgres    │ ────────────────────────▶  │  Airflow Worker 컨테이너 │
│  (원본 테이블) │                            │  /tmp/{table}.parquet    │
└───────────────┘                            └────────────┬─────────────┘
                                                2. 로컬에 임시 파일 생성
                                                        │
                                                 3. PUT (파일 업로드)
                                                        │
                                                        ▼
                                             ┌───────────────────────────────┐
                                             │   Snowflake Stage             │
                                             │   @MY_STAGE/{table}.parquet   │
                                             └────────────┬──────────────────┘
                                                          │
                                                  4. COPY INTO
                                                          │
                                                          ▼
                                              ┌──────────────────────────────┐
                                              │  Snowflake staging 테이블    │
                                              │  {table}_staging             │
                                              └────────────┬─────────────────┘
                                                           │
                                          5. VALIDATE 통과 시 SWAP (Full Load)
                                             / MERGE (CDC)
                                                           ▼
                                              ┌──────────────────────────┐
                                              │  Snowflake 운영 테이블   │
                                              │  {table}                 │
                                              └──────────────────────────┘

정리(cleanup, trigger_rule="all_done") 단계에서 항상 실행:
  - Snowflake Stage 파일 REMOVE   ← Snowflake 쪽 잔여 파일 정리
  - 로컬 /tmp/{table}.parquet 삭제 ← 컨테이너 임시 파일 정리 (finally 블록)
```

이 그림에서 짚을 포인트:

- 데이터는 Postgres에서 읽힌 후 Airflow Worker의 임시 파일을 거쳐 Snowflake Stage와 Snowflake 테이블로 이동한다. 각 이동 구간마다 실패 가능 지점이 존재한다.
- staging 테이블에서 운영 테이블로 넘어가는 마지막 단계(SWAP/MERGE) 직전에 VALIDATE가 게이트로 껴 있어서, 여기서 걸리면 앞의 3번 이동은 이미 다 끝났어도 운영 테이블은 오염되지 않는다.

---

## Part 3. 실습을 통해 정리한 핵심 개념

### 3.1 Logical Date

**정의**

DAG Run을 식별하는 기준 시각으로, 실제 실행 시작 시간이 아니라 해당 실행이 의미하는 스케줄 기준 시각이다.

**Logical Date vs Start Date**

| 구분 | Logical Date | Start Date |
|---|---|---|
| 의미 | 이 실행이 대표하는 시각 (고정된 이름표) | 실제로 태스크가 돌기 시작한 물리적 시각 |
| 재실행 시 | 항상 동일 | 매번 달라질 수 있음 |
| 조건절에 활용 | 가능 (재현 가능) | 지양 (매번 결과가 달라짐 → 안티패턴) |

**실측 결과 (트리거 방식별)**
- 수동 트리거: Logical Date ≈ 트리거 버튼 누른 시각 (Start Date와 초 단위 차이)
- 스케줄 트리거(`*/N * * * *`): Logical Date = cron이 정한 정확한 시각. Start Date는 그보다 살짝 뒤

**버전 관련 확인 사항 (Airflow 3.x)**

Airflow 버전에 따라 `data_interval_start`/`data_interval_end` 기본 동작이 다를 수 있어 직접 로그 확인이 필요했다.

**증분 처리 방식 비교**

| 방식 | 기준 | 동작 | 특징 |
|---|---|---|---|
| Start Date / now() | 실행되는 순간 | 그때그때 다름 | 재현 불가, 실무에서 지양 |
| Logical Date | 정해진 스케줄 구간 | 그 구간만 딱 처리 | 재현 가능(멱등성). 단, 한 번 놓치면 복구 안 됨 → 정산/리포트 등 배치성 작업에 적합 |
| Watermark | 마지막 처리 지점 기록 | 마지막 지점부터 이어서 처리 | 절대 안 놓침 → CDC처럼 누락이 치명적인 작업에 적합 |

**실무 활용**
- 파일/파티션 경로: `{{ ds }}` 등으로 날짜별 폴더 분리 저장
- Backfill(과거 재실행): `catchup=True`로 과거 스케줄 구간들을 각자의 logical_date로 재현 실행
- 멱등성(Idempotency): 같은 logical_date로 몇 번을 재실행해도 항상 같은 구간을 처리 → 결과 예측/재현 가능

### 3.2 DAG Run 연결 (Trigger, Push 방식)

**정의**

하나의 DAG 실행 단위(DAG Run)를 다른 DAG에서 직접 생성하는 방식. Full Load DAG 완료 후 CDC DAG를 자동 실행하도록 DAG 간 실행 흐름을 연결한다.

Airflow에서는 DAG 간 의존성을 직접 연결하지 않고, `TriggerDagRunOperator`를 사용해 특정 DAG 실행을 요청하는 방식으로 구성한다.

**아키텍처 흐름**

```
┌──────────────────────────────────┐
│  postgres_to_snowflake_full_load  (schedule=None, 수동 트리거)
│                                   │
│  get_tables()                    │
│       │                          │
│       ▼                          │
│  ┌─ etl_pipeline (TaskGroup) ─────────────────┐
│  │ create_table → export_parquet → upload     │
│  │   → copy_into → validate → cleanup         │
│  └──────────────────────────────────────────────┘
│       │                          │
│       ▼                          │
│  TriggerDagRunOperator           │   ← full_load 완료 즉시 cdc를 "직접 깨움" (Push)
│  (trigger_dag_id=                │
│   "postgres_to_snowflake_cdc")   │
└──────────┬────────────────────────┘
           │
           ▼
┌──────────────────────────────────┐
│  postgres_to_snowflake_cdc   (schedule="*/15 * * * *")
│                                   │
│  ensure_control_table            │
│       │                          │
│       ▼                          │
│  get_cdc_tables()                │
│       │                          │
│       ▼                          │
│  ┌─ cdc_pipeline (TaskGroup) ─────────────────────────┐
│  │ create_table → export_parquet → upload             │
│  │  → copy_into_staging → validate → merge_cdc        │
│  │  → update_watermark → cleanup                      │
│  └──────────────────────────────────────────────────────┘
└──────────────────────────────────┘
```

**핵심 구현**

`full_load`의 `cleanup_task` 완료 후 `TriggerDagRunOperator`로 `cdc` DAG를 즉시 실행:

```python
trigger_cdc = TriggerDagRunOperator(
    task_id="trigger_cdc_dag",
    trigger_dag_id="postgres_to_snowflake_cdc",
    wait_for_completion=False,
    reset_dag_run=True,
)
cleanup_task >> trigger_cdc
```

**실습 결과**

Full Load DAG 완료 후 `TriggerDagRunOperator`를 통해 CDC DAG가 자동으로 실행되는 것을 확인했다.

즉, Full Load 완료 → CDC 시작 구조로 DAG 간 실행 흐름을 연결할 수 있었다.

### 3.3 Sensor (Pull 방식)

**정의**

"어떤 조건이 참(True)이 될 때까지 반복 확인하다가, 참이 되는 순간 다음 태스크를 진행시키는 태스크"

**Trigger(Push)와의 비교**

| 구분 | Trigger (Push) | Sensor (Pull) |
|---|---|---|
| 주도권 | 앞 DAG가 능동적으로 뒤 DAG를 깨움 | 뒤 DAG가 스스로 조건을 계속 확인하며 대기 |
| 적합한 상황 | 우리가 관리하는 DAG끼리 연결 | 외부 시스템(우리가 통제 못 하는 것)이 조건을 채워줄 때 |

**실무에서 흔한 Sensor 활용 사례**
- 외부 업체가 올리는 파일 도착 대기 (FileSensor, S3KeySensor)
- DB에 특정 조건 충족될 때까지 대기 (SqlSensor)
- 외부 API 응답 확인 (HttpSensor)

**실습 1: 단순 조건 기반 Sensor**

Logical Date나 DB 직접 조회처럼 복잡한 변수를 제거하고, 명확하고 단순한 조건(Postgres에 특정 테이블이 존재하는지)으로 Sensor 개념을 확인:

```python
@task.sensor(mode="reschedule", timeout=60 * 5, poke_interval=20)
def wait_for_new_changes():
    pg = PostgresHook(POSTGRES_CONN_ID)
    exists = pg.get_first("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'sensor_test'
    """)[0]
    return exists > 0
```

**결과: 정상 동작 확인**

```
14:57:13  존재 여부: False   (poke 1)
14:57:34  존재 여부: False   (poke 2)
   ...
14:58:39  존재 여부: False   (poke 6)
14:59:01  존재 여부: True    (poke 7) → Success criteria met. Exiting.
```

`CREATE TABLE sensor_test (id INT);`를 Postgres에서 직접 실행한 시점에 정확히 맞춰 Sensor가 조건 충족을 감지하고 통과하는 것을 확인함.

---

## 결론

이번 실습에서는 PostgreSQL → Snowflake 데이터 파이프라인을 직접 구현하면서 단순히 데이터를 이동시키는 것보다, 운영 환경을 고려한 확장성, 데이터 정합성, 장애 대응 구조를 설계하는 것이 중요하다는 점을 확인했다.

구현 과정에서 중점적으로 고려한 부분은 다음과 같다.

### 1. 확장 가능한 파이프라인 구조

Dynamic Task Mapping을 활용하여 테이블 목록을 기반으로 Task가 동적으로 생성되도록 설계했다.

이를 통해 특정 테이블을 코드에 직접 작성하는 방식이 아니라, 메타데이터 기반으로 동일한 ETL 로직을 여러 테이블에 적용할 수 있도록 구성했다.

결과적으로 테이블 수가 수백 개 규모로 증가하더라도 코드 변경 없이 동일한 구조로 확장 가능한 형태를 구현했다.

### 2. 데이터 안정성을 고려한 적재 방식

데이터 적재 과정에서 실패가 발생하더라도 운영 테이블이 영향을 받지 않도록 staging 기반 구조를 적용했다.

**Full Load:**
1. staging 테이블에 먼저 적재
2. 데이터 검증 수행
3. 검증 성공 시 SWAP으로 운영 테이블 교체

**CDC:**
1. 변경 데이터 적재
2. validate 수행
3. MERGE 적용
4. 성공 이후 watermark 갱신

이를 통해 중간 단계에서 오류가 발생하더라도 운영 데이터의 정합성을 유지하고, 실패 이후 재처리가 가능한 구조를 구성했다.

### 3. Airflow 실행 모델 이해

실습 과정에서 Logical Date, DAG Run Trigger, Sensor를 직접 적용하면서 Airflow가 단순한 Task 실행 도구가 아니라 데이터 파이프라인의 실행 흐름을 관리하는 오케스트레이션 플랫폼임을 확인했다.

- Logical Date를 활용한 스케줄 기준 시간 관리
- TriggerDagRunOperator를 통한 DAG 간 실행 연결
- Sensor를 통한 외부 조건 대기 처리

등을 통해 실제 운영 환경에서 필요한 실행 제어 방식을 이해하는 것이 중요하다.
