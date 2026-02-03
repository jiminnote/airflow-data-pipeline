from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import json

def save_to_db(**context):
    """API 응답을 DB에 저장 (에러 핸들링 추가)"""
    ti = context['ti']
    
    try:
        response = ti.xcom_pull(task_ids='fetch_user')
        
        if not response:
            raise ValueError("API 응답이 비어있습니다!")
        
        data = json.loads(response)
        
        # 필수 필드 검증
        required_fields = ['id', 'name', 'email', 'company']
        for field in required_fields:
            if field not in data:
                raise ValueError(f"필수 필드 '{field}'가 없습니다!")
        
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
        
        print(f"✅ 사용자 '{data['name']}' 저장 성공!")
        
    except json.JSONDecodeError as e:
        print(f"❌ JSON 파싱 실패: {e}")
        raise
    except Exception as e:
        print(f"❌ DB 저장 실패: {e}")
        raise

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'execution_timeout': timedelta(minutes=5),
}

with DAG(
    dag_id='api_with_retry',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['tutorial', 'error-handling']
) as dag:
    
    drop_table = SQLExecuteQueryOperator(
        task_id='drop_table',
        conn_id='postgres_default',
        sql="DROP TABLE IF EXISTS api_users"
    )
    
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres_default',
        sql="""
            CREATE TABLE api_users (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                company VARCHAR(100),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
    )
    
    fetch_user = HttpOperator(
        task_id='fetch_user',
        http_conn_id='api_default',
        endpoint='/users/1',
        method='GET',
        response_filter=lambda response: response.text,
        log_response=True,
        retries=5,
        retry_delay=timedelta(seconds=10),
    )
    
    save_data = PythonOperator(
        task_id='save_to_db',
        python_callable=save_to_db
    )
    
    drop_table >> create_table >> fetch_user >> save_data