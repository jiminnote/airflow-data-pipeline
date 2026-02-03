from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import json

def parse_api_response(**context):
    """API 응답 파싱"""
    ti = context['ti']
    response = ti.xcom_pull(task_ids='fetch_user')
    
    data = json.loads(response)
    print(f"사용자 이름: {data['name']}")
    print(f"이메일: {data['email']}")
    print(f"회사: {data['company']['name']}")

with DAG(
    dag_id='api_test',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['tutorial', 'api']
) as dag:
    
    # Task 1: API 호출
    fetch_user = HttpOperator(
        task_id='fetch_user',
        http_conn_id='api_default',
        endpoint='/users/1',  # 사용자 ID 1번 조회
        method='GET',
        response_filter=lambda response: response.text,
        log_response=True
    )
    
    # Task 2: 응답 파싱
    parse_response = PythonOperator(
        task_id='parse_response',
        python_callable=parse_api_response
    )
    
    fetch_user >> parse_response