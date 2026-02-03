from airflow.timetables.base import Timetable, DagRunInfo
from airflow.plugins_manager import AirflowPlugin
from datetime import datetime, timedelta
from typing import Optional

class BusinessDayTimetable(Timetable):
    """평일만 실행 (주말/공휴일 제외)"""
    
    # 한국 공휴일 (2024년)
    HOLIDAYS = [
        datetime(2024, 1, 1),
        datetime(2024, 2, 9),   
        datetime(2024, 2, 10),
        datetime(2024, 2, 11),
        datetime(2024, 3, 1),  
        datetime(2024, 4, 10),  
        datetime(2024, 5, 5),  
        datetime(2024, 5, 15), 
        datetime(2024, 6, 6),  
        datetime(2024, 8, 15),  
        datetime(2024, 9, 16), 
        datetime(2024, 9, 17),
        datetime(2024, 9, 18),
        datetime(2024, 10, 3), 
        datetime(2024, 10, 9),  
        datetime(2024, 12, 25), 
    ]
    
    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval,
        restriction,
    ) -> Optional[DagRunInfo]:
        
        # 시작 시간 결정
        if last_automated_data_interval is not None:
            next_start = last_automated_data_interval.end
        else:
            next_start = restriction.earliest
        
        # 시간 정규화 (자정으로)
        next_start = next_start.replace(
            hour=9,      # 오전 9시 실행으로 변경
            minute=0,
            second=0,
            microsecond=0
        )
        
        # 다음 평일 찾기 (최대 30일 탐색)
        max_iterations = 30
        iterations = 0
        
        while iterations < max_iterations:
            # 주말 체크
            if next_start.weekday() >= 5:
                next_start += timedelta(days=1)
                iterations += 1
                continue
            
            # 공휴일 체크
            if next_start.date() in [h.date() for h in self.HOLIDAYS]:
                next_start += timedelta(days=1)
                iterations += 1
                continue
            
            # 평일이면서 공휴일이 아님
            break
        
        # 30일 내에 평일을 못 찾으면 None
        if iterations >= max_iterations:
            return None
        
        # 다음 실행까지의 interval
        next_end = next_start + timedelta(days=1)
        
        return DagRunInfo.interval(start=next_start, end=next_end)

# Plugin 등록
class CustomTimetablePlugin(AirflowPlugin):
    name = "custom_timetable_plugin"
    timetables = [BusinessDayTimetable]