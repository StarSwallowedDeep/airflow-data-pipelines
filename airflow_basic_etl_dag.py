from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="airflow_basic_etl_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
)
def airflow_basic_etl_dag():

    @task
    def extract():
        print("데이터 추출 중...")
        return 10

    @task
    def transform(x):
        print("데이터 가공 중...")
        return x * 2

    @task
    def load(result):
        print(f"결과 저장: {result}")

    # Task 연결
    data = extract()
    transformed = transform(data)
    load(transformed)


dag = airflow_basic_etl_dag()
