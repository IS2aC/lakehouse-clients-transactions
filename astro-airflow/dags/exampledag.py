from include.dailtrans_checks import DailyTansactionsChecking as DTC
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from include.elt_dailytrans import localfile_on_minio
from airflow import DAG

### VARIABLES ###

PATH_DAILYTRANS_REPERTORY = '/usr/local/airflow/include/dailytrans'
PATH_DAILYTRANS_LOGGER = '/usr/local/airflow/include/dailytrans_log.txt'
dtc =  DTC(
    path_dailytrans_repertory = PATH_DAILYTRANS_REPERTORY,
    path_dailytrans_logger = PATH_DAILYTRANS_LOGGER
)
MINIO_HOOK =  S3Hook(aws_conn_id="minio_default")
BUCKET_NAME  = 'dailytrans'
#################



with DAG(
    dag_id =  'exampledag', 
    start_date =  datetime(2025, 5, 20),
    schedule_interval = None, 
    catchup = False
    ) as dag:

    check_dailytrans = PythonOperator(
        task_id='check_dailytrans',
        python_callable=dtc.check_on_dailytrans_repertory
    )

    def branch_over_check_dailytrans(**kwargs):
        ti = kwargs['ti']
        check_dailytrans_result = ti.xcom_pull(task_ids='check_dailytrans')
        if check_dailytrans_result['new']:
            return 'elt_dailytrans'
        else:
            return 'stop_dailytrans'
        
    branch_task_over_dailytrans =   BranchPythonOperator(
            task_id='branch_task_over_dailytrans',
            python_callable=branch_over_check_dailytrans,
            provide_context=True,
    )

    elt_dailytrans  = PythonOperator(
        task_id =  'elt_dailytrans', 
        python_callable =  localfile_on_minio, 
        op_args = [PATH_DAILYTRANS_REPERTORY, MINIO_HOOK, BUCKET_NAME]
    )
    stop_dailytrans =  DummyOperator(task_id='stop_dailytrans')


    ########################################################################

    check_dailytrans >> branch_task_over_dailytrans >> [elt_dailytrans, stop_dailytrans]


