from include.dailtrans_checks import DailyTansactionsChecking as DTC
from include.data_quality import DataQualityVerifications as DQV
from include.data_quality import DataQualityCheckSum as DQCS
from include.preprocessing_minio import PreprocessingMinio as PM
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from include.elt_dailytrans import localfile_on_minio
from airflow import DAG
from airflow.utils.task_group import TaskGroup
import os 

### VARIABLES ###

PATH_DAILYTRANS_REPERTORY = '/usr/local/airflow/include/dailytrans'
PATH_DAILYTRANS_LOGGER = '/usr/local/airflow/include/dailytrans_log.txt'
dtc =  DTC(
    path_dailytrans_repertory = PATH_DAILYTRANS_REPERTORY,
    path_dailytrans_logger = PATH_DAILYTRANS_LOGGER
)
MINIO_HOOK =  S3Hook(aws_conn_id="minio_default")
BUCKET_NAME  = 'dailytrans'
BUCKET_NAME_MERCHANTSINFOS = 'merchantsinfos'
LAST_FILE_DETECT  =  max(os.listdir(PATH_DAILYTRANS_REPERTORY)) if len(os.listdir(PATH_DAILYTRANS_REPERTORY)) > 0 else ''
dqv = DQV(PATH_DAILYTRANS_REPERTORY + '/' + LAST_FILE_DETECT)
dqcs_dailytrans = DQCS(
    local_file_path_dataframe = PATH_DAILYTRANS_REPERTORY + '/' + LAST_FILE_DETECT,
    s3_file_path_dataframe = LAST_FILE_DETECT, 
    bucket_name='dailytrans'
)

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
            return 'schema_validation_task'
        else:
            return 'stop_dailytrans'
        
    branch_task_over_dailytrans =   BranchPythonOperator(
            task_id='branch_task_over_dailytrans',
            python_callable=branch_over_check_dailytrans,
            provide_context=True,
    )

    schema_validation_task = PythonOperator(
        task_id='schema_validation_task',
        python_callable=dqv.data_schema_validation,
    )


    def branch_over_check_schema_validation(**kwargs):
        ti = kwargs['ti']
        schema_validation_task_result = ti.xcom_pull(task_ids='schema_validation_task')
        if schema_validation_task_result['success']:
            return 'data_quality_assert'
        else:
            return 'stop_schema_validation_task'
        
    branch_over_check_schema_validation_task =  BranchPythonOperator(
            task_id='branch_over_check_schema_validation_task',
            python_callable=branch_over_check_schema_validation,
            provide_context=True,
    )
    stop_schema_validation_task = DummyOperator(task_id='stop_schema_validation_task')

    with TaskGroup('data_quality_assert') as data_quality_assert:
        null_detections  = PythonOperator(
            task_id='null_detections',
            python_callable=dqv.checks_null_detections
        )
        negative_value_over_amount_transaction = PythonOperator(
            task_id='negative_value_over_amount_transaction',
            python_callable=dqv.checks_negative_value_over_amount_transaction
        )
        value_over_type_transactions = PythonOperator(
            task_id='value_over_type_transactions',
            python_callable=dqv.checks_over_type_transactions
        )
        transaction_date_timestamp = PythonOperator(
            task_id='transaction_date_timestamp',
            python_callable=dqv.checks_over_transaction_date_timestamp
        )

    ############### ELT DAILYTRANS ######################

    elt_dailytrans  = PythonOperator(
        task_id =  'elt_dailytrans', 
        python_callable =  localfile_on_minio, 
        op_args = [PATH_DAILYTRANS_REPERTORY, MINIO_HOOK, BUCKET_NAME]
    )
    stop_dailytrans =  DummyOperator(task_id='stop_dailytrans')


    ############### CHECKSUM DAILYTRANS ###################

    
    checksum_dailytrans = PythonOperator(
        task_id='checksum_dailytrans',
        python_callable=dqcs_dailytrans.data_checksum
    )


    def branch_over_checksum_dailytrans(**kwargs):
        ti = kwargs['ti']
        checksum_dailytrans_result =  ti.xcom_pull(task_ids='checksum_dailytrans')
        if checksum_dailytrans_result['success']:
            return 'list_merchants_of_last_dailytrans'
        else:
            return 'stop_checksum_dailytrans'
        
    branch_over_checksum_task =  BranchPythonOperator(
            task_id='branch_over_checksum_task',
            python_callable=branch_over_checksum_dailytrans,
            provide_context=True,
    )
        
    stop_checksum_dailytrans =  DummyOperator(task_id='stop_checksum_dailytrans')

    ############### preprocessing before sparks job ##
    list_merchants_of_last_dailytrans = PythonOperator(
        task_id='list_merchants_of_last_dailytrans',
        python_callable=PM.list_merchants_of_last_dailytrans,
        op_args=[BUCKET_NAME]
    )

    list_merchants_new_or_old_on_dailytrans =  PythonOperator(
        task_id='list_merchants_new_or_old_on_dailytrans',
        python_callable=PM.list_merchants_new_or_old_on_dailytrans,
        op_args=['list_merchants_of_last_dailytrans', BUCKET_NAME_MERCHANTSINFOS],
        provide_context=True
    )

    def branch_list_merchants_new_or_old_on_dailytrans(**kwargs):
        ti = kwargs['ti']
        list_merchants_new_or_old_on_dailytrans_result =  ti.xcom_pull(task_ids='list_merchants_new_or_old_on_dailytrans')
        if len(list_merchants_new_or_old_on_dailytrans_result['new']) > 0:
            return 'create_new_merchants_path_minio'
        else:
            return 'job_sparks'
    branch_list_merchants_new_or_old_on_dailytrans_task =  BranchPythonOperator(
            task_id='branch_list_merchants_new_or_old_on_dailytrans_task',
            python_callable=branch_list_merchants_new_or_old_on_dailytrans,
            provide_context=True,
    )
    create_new_merchants_path_minio =  PythonOperator(
        task_id='create_new_merchants_path_minio',
        python_callable=PM.create_new_merchants_path_minio,
        op_args=['list_merchants_new_or_old_on_dailytrans', BUCKET_NAME_MERCHANTSINFOS],
        provide_context=True
    )

    ################ sparks job ######################
    job_sparks = DummyOperator(task_id='job_sparks')
    ########################################################################

    check_dailytrans >> branch_task_over_dailytrans >> [schema_validation_task, stop_dailytrans]
    schema_validation_task >> branch_over_check_schema_validation_task >> [data_quality_assert,stop_schema_validation_task]
    data_quality_assert >> elt_dailytrans >> checksum_dailytrans
    checksum_dailytrans >> branch_over_checksum_task >> [list_merchants_of_last_dailytrans, stop_checksum_dailytrans]

    list_merchants_of_last_dailytrans >> list_merchants_new_or_old_on_dailytrans >> branch_list_merchants_new_or_old_on_dailytrans_task
    branch_list_merchants_new_or_old_on_dailytrans_task >> [create_new_merchants_path_minio, job_sparks]
    create_new_merchants_path_minio >> job_sparks