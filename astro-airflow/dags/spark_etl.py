from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator as so
from datetime import datetime, timedelta

with DAG('spark_etl', 
         start_date =  datetime(2025, 5, 20),
         schedule_interval = None) as dag:
    

    job_0 = so(
        task_id='job_0',
        application='include/job0.py',
        conf={'spark.master': 'spark://da-spark-master:7077'},
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='2g',
        driver_memory='2g',
        num_executors='1',
        verbose=False
    )

    job_0