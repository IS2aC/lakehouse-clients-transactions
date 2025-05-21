
def localfile_on_minio(files_daily_trans_repertory, s3_hook, bucket_name, **kwargs):
    """
    Function for uploading localcsv or what file on minio bucket.
        - file_local_path : path to local file.
        - s3_hook : airflow hook object for minio connection.
        - bucket_name : minio bucket.
        - s3_key : destination minio file on bucket.
    """
    client = s3_hook.get_conn()  # boto3 action
    ti = kwargs['ti']
    check_dailytrans_result = ti.xcom_pull(task_ids='check_dailytrans')
    s3_key = check_dailytrans_result['filename']
    file_local_path = files_daily_trans_repertory + '/' +  s3_key

    if not s3_hook.check_for_bucket(bucket_name):
        client.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' has been created !")


    # loading
    s3_hook.load_file(filename=file_local_path, key=s3_key, bucket_name=bucket_name, replace=True)

    print(f"File '{file_local_path}' upload on bucket name  : '{bucket_name}' with path :  '{bucket_name}/{s3_key}'.")