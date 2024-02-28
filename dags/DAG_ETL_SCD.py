from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Mohamed_Younes',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='DAG_ETL_v1.0',
    default_args=default_args,
    start_date=datetime(2024, 2, 27),
    schedule_interval='0 0 * * *' #every day
) as dag:
 
    Extract = BashOperator(
        task_id='Extract_v1',
        bash_command='python /opt/airflow/app/Scripts/Extract.py',
    )
    
    Transform = BashOperator(
        task_id='Transform_v1',
        bash_command='python /opt/airflow/app/Scripts/Transform.py',
    )
    
    Load = BashOperator(
        task_id='Load_v1',
        bash_command='python /opt/airflow/app/Scripts/ETL.py',
    )
    
    Extract >> Transform >> Load
    
