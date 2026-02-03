from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Plugin에서 정의한 Timetable import
from custom_timetable import BusinessDayTimetable

with DAG(
    dag_id='scheduling_dag',
    timetable=BusinessDayTimetable(),
    start_date=datetime(2024, 1, 1),
    max_active_runs=1,  # 동시 실행 방지 (depends_on_past=True와 함께)
    catchup=False,
    tags=['production', 'business-day'],
    default_args={
        'owner': 'data-team',
        'depends_on_past': True,  # 이전 실행 성공해야 다음 실행
        'execution_timeout': timedelta(minutes=30),
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'email': ['alerts@company.com'],
        'email_on_failure': True,
        'email_on_retry': False,
    }
) as dag:
    
    # 예시 Task
    start = BashOperator(
        task_id='start',
        bash_command='echo "Business day pipeline started"'
    )
    
    process = BashOperator(
        task_id='process_data',
        bash_command='echo "Processing data..."'
    )
    
    end = BashOperator(
        task_id='end',
        bash_command='echo "Pipeline completed"'
    )
    
    start >> process >> end