from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # TriggerDagRunOperator

from datetime import datetime
# dag a
"""
 trigger dag run operator를 사용하면 기본적으로 다음 task로 이동하기 전에 triggerdag가 끝나길 기다리지 않는다.

 중요!
 trigger dag를 실행할때 dag_run_conf trigger로 실행해야됨
"""

default_args = {
    'start_date': datetime(2021, 12, 1)
}

def _downloading(): 
    print('downloading')

with DAG('trigger_dag', 
    schedule_interval='@daily', 
    default_args=default_args, 
    catchup=False) as dag:

    downloading = PythonOperator( # 데이터를 다운로드
        task_id='downloading',
        python_callable=_downloading
    )
    
    trigger_target = TriggerDagRunOperator(
        task_id="trigger_target", 
        trigger_dag_id="target_dag", # trigger 하고 싶은 dag id
        execution_date="{{ ds }}", # 현 excution date
        # [problem]
        # trigger_dag 와 target_dag 사이에 execution date가 다를 경우 dag b에서 저장된 데이터를 다룰 수 없다.
        # => trigger_dag 와 target_dag가 같은 execution date를 공유하도록 해야함.
        reset_dag_run=True, 
        # already exist for dag id target_dag 에러 출력
        # => 같은 execution date의 dag_run을 두번 실행할 수 없음.
        
        wait_for_completion=True,
        poke_interval=30
        # 30초 마다 TriggerDagRunOperator는 TriggerDag가 끝났는지 아닌지를 체크한다.
        
    )
