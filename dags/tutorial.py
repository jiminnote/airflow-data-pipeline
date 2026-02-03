import textwrap
from datetime import datetime, timedelta

# Operators
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator  

from airflow.sdk import DAG

# Python 함수 정의
def print_hello():
    print("Hello from Python!")
    return "Success!"

def print_context(**context):
    """execution_date 등 context 정보 출력"""
    print(f"Execution Date: {context['ds']}")
    print(f"Task Instance: {context['task_instance']}")
    print(f"DAG: {context['dag']}")
    return context['ds']

with DAG(
    "tutorial",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    t1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    t2 = BashOperator(
        task_id="sleep",
        depends_on_past=False,
        bash_command="sleep 5",
        retries=3,
    )

    # PythonOperator 추가
    t4 = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello,
    )

    t5 = PythonOperator(
        task_id="print_context",
        python_callable=print_context,
    )

    templated_command = textwrap.dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )

    t3 = BashOperator(
        task_id="templated",
        depends_on_past=False,
        bash_command=templated_command,
    )

    # Task 의존성 설정
    t1 >> [t2, t3, t4]
    t4 >> t5