# Airflow Data Pipeline

Apache Airflow 기반 데이터 파이프라인 프로젝트입니다.  
다양한 ETL 패턴과 실무에서 자주 사용하는 기능들을 구현했습니다.

## 프로젝트 목표

- Airflow를 활용한 ETL 파이프라인 구축
- API 데이터 수집 → DB 적재 자동화
- 에러 핸들링 및 재시도 로직 구현
- 한국 공휴일을 고려한 커스텀 스케줄링
- Docker 기반 로컬 개발 환경 구성

## 프로젝트 구조
```
airflow-data-pipeline/
├── dags/
│   ├── advanced_weather_pipeline.py  # 다중 도시 날씨 데이터 수집
│   ├── api_to_db.py                  # API → PostgreSQL ETL
│   ├── api_with_retry.py             # 에러 핸들링 + 재시도
│   ├── api_branch_test.py            # 조건 분기 처리
│   ├── api_failure_test.py           # 실패 시나리오 테스트
│   ├── scheduling_dag.py             # 스케줄링 예제
│   ├── weather_pipeline.py           # 기본 날씨 파이프라인
│   └── tutorial.py                   # Airflow 튜토리얼
├── plugins/
│   ├── custom_timetable.py           # 한국 공휴일 제외 스케줄러
│   └── weather_utils.py              # 날씨 API 유틸리티
├── docker-compose.yaml               # Airflow 클러스터 구성
└── .gitignore
```

## 주요 기능

### 1. 날씨 데이터 파이프라인
- OpenWeatherMap API 연동
- 서울, 도쿄, 뉴욕 등 다중 도시 데이터 수집
- TaskGroup을 활용한 병렬 처리
- XCom을 통한 태스크 간 데이터 전달

### 2. API → DB ETL
- REST API 데이터 추출
- PostgreSQL 적재 (Upsert 처리)
- 데이터 검증 및 변환

### 3. 에러 핸들링
- 자동 재시도 (retry)
- 필수 필드 검증
- 실패 시 알림 콜백

### 4. 한국 공휴일 스케줄러
- 평일만 실행 (주말 제외)
- 한국 공휴일 제외 (설날, 추석 등)
- Custom Timetable 구현

## 기술 스택

- **Orchestration**: Apache Airflow 2.x
- **Executor**: CeleryExecutor
- **Database**: PostgreSQL
- **Message Broker**: Redis
- **Container**: Docker, Docker Compose
- **Language**: Python 3.x

## 실행 방법
```bash
# 1. 저장소 클론
git clone https://github.com/jiminnote/airflow-data-pipeline.git
cd airflow-data-pipeline

# 2. 환경 변수 설정
cp .env.example .env
# .env 파일에서 필요한 값 수정

# 3. Docker Compose 실행
docker-compose up -d

# 4. Airflow 웹 UI 접속
# http://localhost:8080
# 기본 계정: airflow / airflow
```

## 학습 포인트

| 주제 | 파일 | 설명 |
|------|------|------|
| ETL 기본 패턴 | `api_to_db.py` | Extract → Transform → Load |
| 에러 핸들링 | `api_with_retry.py` | retry, on_failure_callback |
| 조건 분기 | `api_branch_test.py` | BranchPythonOperator |
| 병렬 처리 | `advanced_weather_pipeline.py` | TaskGroup, Dynamic Task |
| 커스텀 스케줄링 | `custom_timetable.py` | Timetable 플러그인 |

## 관련 프로젝트

- [ecommerce-kafka-project](https://github.com/jiminnote/ecommerce-kafka-project) - Kafka 기반 이벤트 드리븐 시스템
