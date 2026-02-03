from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import json

def save_to_db(**context):
    """API 응답을 파싱하고 DB에 저장"""
    ti = context['ti']
    response = ti.xcom_pull(task_ids='fetch_user')
    data = json.loads(response)
    
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    hook.run("""
        INSERT INTO api_users VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            email = EXCLUDED.email,
            company = EXCLUDED.company,
            updated_at = CURRENT_TIMESTAMP
    """, parameters=(
        data['id'],
        data['name'],
        data['email'],
        data['company']['name']
    ))
    
    print(f"✅ 사용자 '{data['name']}' 데이터 저장 완료!")

def print_db_data(**context):
    """DB에 저장된 데이터 확인"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    records = hook.get_records("SELECT * FROM api_users ORDER BY id")
    
    print("\n📊 저장된 사용자 목록:")
    for record in records:
        print(f"ID: {record[0]} | 이름: {record[1]} | 이메일: {record[2]}")

with DAG(
    dag_id='api_to_db',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['tutorial', 'pipeline']
) as dag:
    
    # Task 1: 기존 테이블 삭제
    drop_table = SQLExecuteQueryOperator(
        task_id='drop_table',
        conn_id='postgres_default',
        sql="DROP TABLE IF EXISTS api_users"
    )
    
    # Task 2: 테이블 생성
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS api_users (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                company VARCHAR(100),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
    )
    
    # Task 3: API 호출
    fetch_user = HttpOperator(
        task_id='fetch_user',
        http_conn_id='api_default',
        endpoint='/users/1',
        method='GET',
        response_filter=lambda response: response.text,
        log_response=True
    )
    
    # Task 4: DB 저장
    save_data = PythonOperator(
        task_id='save_to_db',
        python_callable=save_to_db
    )
    
    # Task 5: 결과 확인
    verify_data = PythonOperator(
        task_id='verify_data',
        python_callable=print_db_data
    )
    
    # 의존성 설정 (drop_table 추가!)
    drop_table >> create_table >> fetch_user >> save_data >> verify_data