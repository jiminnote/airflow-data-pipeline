from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import json

def handle_response(**context):
    """API 응답 처리"""
    ti = context['ti']
    response = ti.xcom_pull(task_ids='fetch_invalid_user')
    
    if response:
        data = json.loads(response)
        print(f"✅ 데이터: {data}")
    else:
        print("❌ 응답이 비어있습니다!")

default_args = {
    'owner': 'airflow',
    'retries': 3,  # 3번 재시도
    'retry_delay': timedelta(seconds=5),  # 5초 대기
}

with DAG(
    dag_id='api_failure_test',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['tutorial', 'test']
) as dag:
    
    # 존재하지 않는 사용자 ID (404 에러 발생)
    fetch_invalid_user = HttpOperator(
        task_id='fetch_invalid_user',
        http_conn_id='api_default',
        endpoint='/users/99999',  # ← 존재하지 않는 ID
        method='GET',
        response_filter=lambda response: response.text,
        log_response=True,
        retries=3,
        retry_delay=timedelta(seconds=5),
    )
    
    handle_response = PythonOperator(
        task_id='handle_response',
        python_callable=handle_response
    )
    
    fetch_invalid_user >> handle_response