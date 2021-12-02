from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime
# dag b
default_args = {
    'start_date': datetime(2021, 12, 1)
}

def _cleaning():
    print('Clearning from target DAG')

with DAG('target_dag', 
    schedule_interval='@daily', 
    default_args=default_args, 
    catchup=False) as dag:
 
    storing = BashOperator(  # 다운로드 된 데이터를 DB에 저장
        task_id='storing',
        bash_command='sleep 30' # trigger_dag의 poke_interval은 30초
    )

    cleaning = PythonOperator(
        task_id='cleaning',
        python_callable=_cleaning
    )

    storing >> cleaning