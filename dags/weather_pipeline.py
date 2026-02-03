from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import json

API_KEY = '5f89bcebb90262564e8d1193a28933e0'
# 수집할 도시 목록 (공백 제거!)
CITIES = [
    {'name': 'Seoul', 'query': 'Seoul'},
    {'name': 'Tokyo', 'query': 'Tokyo'},
    {'name': 'New_York', 'query': 'New York'},  # ← Task ID용 / API 쿼리용 분리
    {'name': 'London', 'query': 'London'},
    {'name': 'Paris', 'query': 'Paris'}
]

def extract_weather_data(city_name, city_query, **context):
    """API 응답에서 필요한 데이터만 추출"""
    ti = context['ti']
    response = ti.xcom_pull(task_ids=f'fetch_weather_{city_name}')
    
    if not response:
        print(f"❌ {city_query} 데이터 없음")
        return None
    
    data = json.loads(response)
    
    # 필요한 정보만 추출
    weather_info = {
        'city': city_query,  # 표시용은 원래 이름
        'temperature': data['main']['temp'],
        'feels_like': data['main']['feels_like'],
        'humidity': data['main']['humidity'],
        'description': data['weather'][0]['description'],
        'wind_speed': data['wind']['speed'],
        'collected_at': datetime.now().isoformat()
    }
    
    print(f"📊 {city_query}: {weather_info['temperature']}°C, {weather_info['description']}")
    
    return weather_info

def save_all_weather_data(**context):
    """모든 도시의 날씨 데이터를 DB에 저장"""
    ti = context['ti']
    
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    saved_count = 0
    
    for city in CITIES:
        weather_data = ti.xcom_pull(task_ids=f'extract_{city["name"]}')
        
        if not weather_data:
            continue
        
        try:
            hook.run("""
                INSERT INTO weather_data 
                (city, temperature, feels_like, humidity, description, wind_speed, collected_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, parameters=(
                weather_data['city'],
                weather_data['temperature'],
                weather_data['feels_like'],
                weather_data['humidity'],
                weather_data['description'],
                weather_data['wind_speed'],
                weather_data['collected_at']
            ))
            
            saved_count += 1
            print(f"✅ {city['query']} 데이터 저장 완료")
            
        except Exception as e:
            print(f"❌ {city['query']} 저장 실패: {e}")
    
    print(f"\n📊 총 {saved_count}/{len(CITIES)} 도시 데이터 저장됨")

def generate_daily_report(**context):
    """일일 날씨 리포트 생성"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 오늘 수집된 데이터 조회
    records = hook.get_records("""
        SELECT city, temperature, description, collected_at
        FROM weather_data
        WHERE DATE(collected_at) = CURRENT_DATE
        ORDER BY temperature DESC
    """)
    
    if not records:
        print("⚠️ 오늘 수집된 데이터가 없습니다.")
        return
    
    print("\n" + "="*50)
    print(f"📋 일일 날씨 리포트 - {datetime.now().strftime('%Y-%m-%d')}")
    print("="*50)
    
    for idx, record in enumerate(records, 1):
        city, temp, desc, collected = record
        print(f"{idx}. {city:15s} {temp:6.1f}°C  {desc}")
    
    print("="*50)
    
    # 통계 정보
    temps = [r[1] for r in records]
    print(f"\n📊 통계:")
    print(f"   최고 기온: {max(temps):.1f}°C ({records[0][0]})")
    print(f"   최저 기온: {min(temps):.1f}°C ({records[-1][0]})")
    print(f"   평균 기온: {sum(temps)/len(temps):.1f}°C")

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='weather_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['production', 'weather']
) as dag:
    
    # Task 1: 테이블 생성
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                city VARCHAR(50),
                temperature FLOAT,
                feels_like FLOAT,
                humidity INT,
                description VARCHAR(100),
                wind_speed FLOAT,
                collected_at TIMESTAMP
            )
        """
    )
    
    # Task 2-6: 각 도시별 날씨 데이터 수집
    fetch_tasks = []
    extract_tasks = []
    
    for city in CITIES:
        # API 호출
        fetch = HttpOperator(
            task_id=f'fetch_weather_{city["name"]}',
            http_conn_id='weather_api',
            endpoint=f'/data/2.5/weather?q={city["query"]}&appid={API_KEY}&units=metric',  # ← demo를 {API_KEY}로 변경!
            method='GET',
            response_filter=lambda response: response.text,
            log_response=True
        )
        
        # 데이터 추출
        extract = PythonOperator(
            task_id=f'extract_{city["name"]}',
            python_callable=extract_weather_data,
            op_kwargs={'city_name': city['name'], 'city_query': city['query']}
        )
        
        fetch >> extract
        
        fetch_tasks.append(fetch)
        extract_tasks.append(extract)
    
    # Task 7: 모든 데이터 DB 저장
    save_data = PythonOperator(
        task_id='save_to_database',
        python_callable=save_all_weather_data
    )
    
    # Task 8: 일일 리포트 생성
    report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_daily_report
    )
    
    # 의존성 설정
    create_table >> fetch_tasks
    extract_tasks >> save_data >> report