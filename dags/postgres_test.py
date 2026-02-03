from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator

def print_result(**context):
    ti = context['task_instance']
    result = ti.xcom_pull(task_ids='query_table')
    print(f"Query result: {result}")

with DAG(
    "postgres_test",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="PostgreSQL Connection Test",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["database", "postgres"],
) as dag:

    # Task 0: 기존 테이블 삭제 (추가)
    drop_table = SQLExecuteQueryOperator(
        task_id="drop_table",
        conn_id="postgres_default",
        sql="DROP TABLE IF EXISTS test_table;",
    )

    # Task 1: 테이블 생성
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )

    # Task 2: 데이터 삽입
    insert_data = SQLExecuteQueryOperator(
        task_id="insert_data",
        conn_id="postgres_default",
        sql="""
            INSERT INTO test_table (name) 
            VALUES 
                ('Alice'),
                ('Bob'),
                ('Charlie');
        """,
    )

    # Task 3: 데이터 조회
    query_table = SQLExecuteQueryOperator(
        task_id="query_table",
        conn_id="postgres_default",
        sql="SELECT * FROM test_table;",
        do_xcom_push=True,
    )

    # Task 4: 결과 출력
    print_results = PythonOperator(
        task_id="print_results",
        python_callable=print_result,
    )

    # Task 의존성 (drop_table 추가)
    drop_table >> create_table >> insert_data >> query_table >> print_results