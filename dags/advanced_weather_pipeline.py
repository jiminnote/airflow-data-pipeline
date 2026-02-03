from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from datetime import datetime, timedelta
import json

OPENWEATHER_API_KEY = Variable.get("openweather_api_key")
CITIES = ['Seoul', 'Tokyo', 'Beijing', 'Bangkok', 'Singapore', 'New York']

def extract_weather_data(ti, city: str):
    """1. 날씨 데이터 추출"""
    print(f"🌍 Extracting weather for {city}...")
    
    api = WeatherAPI(OPENWEATHER_API_KEY)
    raw_data = api.get_weather(city)
    
    if raw_data:
        ti.xcom_push(key=f'raw_{city}', value=raw_data)
        print(f"✅ Successfully extracted {city}")
        return raw_data
    else:
        raise Exception(f"Failed to extract weather for {city}")
    
def validate_weather_data(ti, city: str):
    """2. 데이터 검증"""
    print(f"🔍 Validating weather data for {city}...")
    
    raw_data = ti.xcom_pull(key=f'raw_{city}',   task_ids=f'city_group_{city}.extract_{city}')
    
    validator = WeatherValidator()
    is_valid, message = validator.validate(raw_data, city)
    
    if is_valid:
        print(f"✅ Valid data for {city}")
        ti.xcom_push(key=f'valid_{city}', value=True)
        return True
    else:
        print(f"❌ Invalid data for {city}: {message}")
        raise ValueError(f"Validation failed for {city}: {message}")

def transform_weather_data(ti, city: str):
    """3. 데이터 변환"""
    print(f"🔄 Transforming weather data for {city}...")
    
    raw_data = ti.xcom_pull(key=f'raw_{city}', task_ids=f'city_group_{city}.extract_{city}')
    
    processor = WeatherProcessor()
    clean_data = processor.transform_data(raw_data)
    
    ti.xcom_push(key=f'clean_{city}', value=clean_data)
    print(f"✅ Transformed {city}: {clean_data['weather_category']}")
    
    return clean_data

def categorize_weather(ti, city: str):
    """4. 날씨 상태별 분기"""
    clean_data = ti.xcom_pull(
        key=f'clean_{city}',
        task_ids=f'city_group_{city}.transform_{city}'
    )
    
    category = clean_data['weather_category']
    print(f"🌤️ {city} weather category: {category}")
    
    # 카테고리별로 다른 task 반환
    task_mapping = {
        'sunny': f'city_group_{city}.process_sunny_{city}',
        'rainy': f'city_group_{city}.process_rainy_{city}',
        'snowy': f'city_group_{city}.process_snowy_{city}',
        'cloudy': f'city_group_{city}.process_cloudy_{city}',
        'other': f'city_group_{city}.process_other_{city}'
    }

    return task_mapping.get(category, f'city_group_{city}.process_other_{city}')

def process_sunny_weather(city: str):
    """맑은 날씨 처리"""
    print(f"☀️ {city}: Sunny weather processing")
    print(f"   - UV index check recommended")
    print(f"   - Outdoor activity suitable")
    return f"{city}_sunny_processed"

def process_rainy_weather(city: str):
    """비 오는 날씨 처리"""
    print(f"🌧️ {city}: Rainy weather processing")
    print(f"   - Umbrella alert sent")
    print(f"   - Flood warning check")
    return f"{city}_rainy_processed"

def process_snowy_weather(city: str):
    """눈 오는 날씨 처리"""
    print(f"❄️ {city}: Snowy weather processing")
    print(f"   - Traffic delay warning")
    print(f"   - Cold weather alert")
    return f"{city}_snowy_processed"

def process_cloudy_weather(city: str):
    """흐린 날씨 처리"""
    print(f"☁️ {city}: Cloudy weather processing")
    print(f"   - Standard weather conditions")
    return f"{city}_cloudy_processed"

def process_other_weather(city: str):
    """기타 날씨 처리"""
    print(f"🌫️ {city}: Other weather processing")
    return f"{city}_other_processed"

def load_to_database(ti, ds, city: str):
    """5. 데이터베이스 적재 (시뮬레이션)"""
    print(f"💾 Loading {city} data to database...")
    
    clean_data = ti.xcom_pull(
        key=f'clean_{city}',
        task_ids=f'city_group_{city}.transform_{city}'
    )
    
    # 실제로는 PostgresHook 사용
    # from airflow.providers.postgres.hooks.postgres import PostgresHook
    # pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    # pg_hook.run(sql=..., parameters=...)
    
    print(f"✅ Saved to DB: {clean_data['city']} - {clean_data['temperature']}°C")
    print(f"   Date: {ds}")
    
    return f"{city}_loaded"

def aggregate_all_cities(ti):
    """6. 모든 도시 데이터 집계"""
    print("📊 Aggregating all city data...")
    
    all_data = []
    for city in CITIES:
        try:
            data = ti.xcom_pull(
                key=f'clean_{city}',
                task_ids=f'city_group_{city}.transform_{city}'
            )
            if data:
                all_data.append(data)
        except Exception as e:
            print(f"⚠️ Could not aggregate {city}: {e}")
    
    # 통계 계산
    if all_data:
        avg_temp = sum(d['temperature'] for d in all_data) / len(all_data)
        avg_humidity = sum(d['humidity'] for d in all_data) / len(all_data)
        
        print(f"📈 Statistics:")
        print(f"   - Cities processed: {len(all_data)}/{len(CITIES)}")
        print(f"   - Average temperature: {avg_temp:.1f}°C")
        print(f"   - Average humidity: {avg_humidity:.1f}%")
        
        # 카테고리별 도시 수
        categories = {}
        for d in all_data:
            cat = d['weather_category']
            categories[cat] = categories.get(cat, 0) + 1
        
        print(f"   - Weather distribution: {categories}")
        
        ti.xcom_push(key='summary', value={
            'total_cities': len(all_data),
            'avg_temp': avg_temp,
            'avg_humidity': avg_humidity,
            'categories': categories
        })
    
    return all_data

def send_summary_report(ti, ds):
    """7. 요약 리포트 발송"""
    print("📧 Sending summary report...")
    
    summary = ti.xcom_pull(key='summary', task_ids='aggregate')
    
    if summary:
        print(f"""
        ═══════════════════════════════════
        🌍 Weather Pipeline Summary
        Date: {ds}
        ═══════════════════════════════════
        ✅ Cities Processed: {summary['total_cities']}
        🌡️  Avg Temperature: {summary['avg_temp']:.1f}°C
        💧 Avg Humidity: {summary['avg_humidity']:.1f}%
        
        Weather Distribution:
        {json.dumps(summary['categories'], indent=2)}
        ═══════════════════════════════════
        """)
    
    # 실제로는 EmailOperator 또는 SlackWebhookOperator 사용
    return "Report sent"

def failure_callback(context):
    """실패 시 콜백"""
    task_id = context['task_instance'].task_id
    dag_id = context['task_instance'].dag_id
    execution_date = context['execution_date']
    exception = context.get('exception')
    
    print(f"""
    ❌ Task Failed!
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    DAG: {dag_id}
    Task: {task_id}
    Execution Date: {execution_date}
    Error: {exception}
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    🔔 Alert sent to Slack/Email
    """)
    
    # 실제로는 SlackWebhookOperator 사용
    # slack_alert = SlackWebhookOperator(...)

# ========== DAG 정의 ==========

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email': ['alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,  # 3번 재시도
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
    'on_failure_callback': failure_callback,  # 실패 시 콜백
}

with DAG(
    dag_id='advanced_weather_pipeline',
    default_args=default_args,
    description='Advanced weather data pipeline with validation and branching',
    schedule_interval='0 */3 * * *',  # 3시간마다
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['production', 'weather', 'etl'],
    max_active_runs=1,
) as dag:
    
    # 시작
    start = BashOperator(
        task_id='start',
        bash_command='echo "🚀 Weather pipeline started at $(date)"'
    )
    
    # 도시별 TaskGroup 생성
    city_groups = []
    
    for city in CITIES:
        with TaskGroup(f'city_group_{city}', tooltip=f'Process {city} weather') as city_group:
            
            # 1. 추출
            extract = PythonOperator(
                task_id=f'extract_{city}',
                python_callable=extract_weather_data,
                op_kwargs={'city': city},
            )
            
            # 2. 검증
            validate = PythonOperator(
                task_id=f'validate_{city}',
                python_callable=validate_weather_data,
                op_kwargs={'city': city},
            )
            
            # 3. 변환
            transform = PythonOperator(
                task_id=f'transform_{city}',
                python_callable=transform_weather_data,
                op_kwargs={'city': city},
            )
            
            # 4. 날씨 카테고리별 분기
            branch = BranchPythonOperator(
                task_id=f'branch_{city}',
                python_callable=categorize_weather,
                op_kwargs={'city': city},
            )
            
            # 5. 카테고리별 처리 Task들
            sunny = PythonOperator(
                task_id=f'process_sunny_{city}',
                python_callable=process_sunny_weather,
                op_kwargs={'city': city},
            )
            
            rainy = PythonOperator(
                task_id=f'process_rainy_{city}',
                python_callable=process_rainy_weather,
                op_kwargs={'city': city},
            )
            
            snowy = PythonOperator(
                task_id=f'process_snowy_{city}',
                python_callable=process_snowy_weather,
                op_kwargs={'city': city},
            )
            
            cloudy = PythonOperator(
                task_id=f'process_cloudy_{city}',
                python_callable=process_cloudy_weather,
                op_kwargs={'city': city},
            )
            
            other = PythonOperator(
                task_id=f'process_other_{city}',
                python_callable=process_other_weather,
                op_kwargs={'city': city},
            )
            
            # 6. 데이터 적재
            load = PythonOperator(
                task_id=f'load_{city}',
                python_callable=load_to_database,
                op_kwargs={'city': city},
                trigger_rule='none_failed_min_one_success',  # Branch 이후
            )
            
            # TaskGroup 내부 의존성
            extract >> validate >> transform >> branch
            branch >> [sunny, rainy, snowy, cloudy, other] >> load
        
        city_groups.append(city_group)
    
    # 집계
    aggregate = PythonOperator(
        task_id='aggregate',
        python_callable=aggregate_all_cities,
        trigger_rule='none_failed_min_one_success',  # 일부 실패해도 집계
    )
    
    # 리포트
    report = PythonOperator(
        task_id='send_report',
        python_callable=send_summary_report,
    )
    
    # 종료
    end = BashOperator(
        task_id='end',
        bash_command='echo "✅ Weather pipeline completed at $(date)"',
        trigger_rule='none_failed_min_one_success',
    )
    
    # ========== 전체 의존성 ==========
    start >> city_groups >> aggregate >> report >> end
