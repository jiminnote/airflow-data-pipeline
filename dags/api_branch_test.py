from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import json

def check_api_response(**context):
    """API 응답 확인 후 분기"""
    ti = context['ti']
    response = ti.xcom_pull(task_ids='fetch_user')
    
    try:
        data = json.loads(response)
        
        # 데이터 검증
        if 'email' in data and '@' in data['email']:
            print(f"✅ 정상 데이터: {data['name']}")
            return 'process_valid_data'
        else:
            print("⚠️ 이메일 형식 오류")
            return 'handle_invalid_data'
            
    except Exception as e:
        print(f"❌ 에러 발생: {e}")
        return 'handle_error'

def process_valid(**context):
    """정상 데이터 처리"""
    ti = context['ti']
    response = ti.xcom_pull(task_ids='fetch_user')
    data = json.loads(response)
    print(f"📊 {data['name']}님의 데이터 처리 완료!")

def handle_invalid(**context):
    """비정상 데이터 처리"""
    print("⚠️ 데이터 품질팀에 알림 전송")

def handle_error(**context):
    """에러 처리"""
    print("❌ 관리자에게 에러 알림 전송")

with DAG(
    dag_id='api_branch_test',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['tutorial', 'branch']
) as dag:
    
    fetch_user = HttpOperator(
        task_id='fetch_user',
        http_conn_id='api_default',
        endpoint='/users/1',
        method='GET',
        response_filter=lambda response: response.text,
        log_response=True
    )
    
    # 조건부 분기
    check_response = BranchPythonOperator(
        task_id='check_response',
        python_callable=check_api_response
    )
    
    process_valid_data = PythonOperator(
        task_id='process_valid_data',
        python_callable=process_valid
    )
    
    handle_invalid_data = PythonOperator(
        task_id='handle_invalid_data',
        python_callable=handle_invalid
    )
    
    handle_error_task = PythonOperator(
        task_id='handle_error',
        python_callable=handle_error
    )
    
    # 의존성: fetch → check → [정상 | 비정상 | 에러]
    fetch_user >> check_response >> [process_valid_data, handle_invalid_data, handle_error_task]