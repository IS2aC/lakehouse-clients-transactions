from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator as ssh
from datetime import datetime, timedelta

with DAG(
    dag_id =  'spark_etl', 
    start_date =  datetime(2025, 5, 20),
    schedule_interval = None, 
    catchup = False
    ) as dag:
    

    job_0 = ssh(
        task_id='job_0',
        ssh_conn_id='ssh_spark_master',
        command='python3 /opt/spark/apps/job_hello_world.py',
    )

    job_0