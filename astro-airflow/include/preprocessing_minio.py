import boto3
import pandas as pd
from io import StringIO


# Configuration MinIO
MINIO_END_POINT = "http://minio:9000"  
MINIO_ACCESS_KEY = "64MmRGXPfPqrRcb10oFJ"
MINIO_SECRET_KEY = "OjOM5BfW0EnNbQvrm8PX5VuSoNognOOOTuJIfvrW"
s3 = boto3.client(
                "s3",
                endpoint_url=MINIO_END_POINT,
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
                region_name="us-east-1",  # important
        )

class PreprocessingMinio: 
    @staticmethod
    # Configuration MinIO
    def read_of_s3_last_dailytrans_ingest(bucket_name)->pd.DataFrame:
            """
            Read the S3 file and return a dataframe.
            """

            # Lister tous les objets du bucket
            response = s3.list_objects_v2(Bucket=bucket_name)
            if "Contents" in response:
                files =  [{'Key':obj['Key'], 'LastModified':obj['LastModified']} for obj in response["Contents"]]
            else:
                files = []
            
            if len(files) >0:
                small_dataf =  pd.DataFrame(files)
                last_file = small_dataf[small_dataf['LastModified'] == small_dataf['LastModified'].max()]['Key'].values[0]
                # read  last object
                response = s3.get_object(Bucket=bucket_name, Key=last_file)
                csv_data = response['Body'].read().decode('utf-8')  # Si encodé en UTF-8

                # Chargement dans une DataFrame
                df = pd.read_csv(StringIO(csv_data))
                return df
            
            return None

    @staticmethod
    def list_merchants_of_last_dailytrans(bucket_name)-> dict:
        # firstly read last file on bucket
        last_dataframe =  PreprocessingMinio.read_of_s3_last_dailytrans_ingest(bucket_name)

        return {'all' : last_dataframe['client_idenfication'].unique().tolist()}

    @staticmethod
    def list_merchants_new_or_old_on_dailytrans(task_ids_before, bucket_name, **kwargs) -> dict:
        ti = kwargs['ti']
        list_merchants_of_last_dailytrans =  ti.xcom_pull(task_ids=task_ids_before)
        list_merchants_of_last_dailytrans = list_merchants_of_last_dailytrans['all']

        

        # List merchants already or not exist on bucket.
        response = s3.list_objects_v2(Bucket=bucket_name)
        if "Contents" in response:
            merchant_in_bucket_merchantsinfos=  [obj['Key'] for obj in response["Contents"]]
        else:
            merchant_in_bucket_merchantsinfos = []

        if len(merchant_in_bucket_merchantsinfos) == 0:
            # all are new merchants
            print('--- all are new merchants ---')
            return {'old': [], 'new': list_merchants_of_last_dailytrans}
        else: 

            # new merchants
            new_merchants =  list(set(list_merchants_of_last_dailytrans) - set(merchant_in_bucket_merchantsinfos))
            # old merchants
            old_merchants =  list(set(list_merchants_of_last_dailytrans) & set(merchant_in_bucket_merchantsinfos))

            # verification 
            assert set(old_merchants) | set(new_merchants)  == set(list_merchants_of_last_dailytrans)
        
            return {'old': old_merchants, 'new':new_merchants}

    @staticmethod
    def create_new_merchants_path_minio(task_ids_before, bucket_name, **kwargs):
        ti = kwargs['ti']
        list_merchants_new_or_old =  ti.xcom_pull(task_ids=task_ids_before)
        list_merchants_new =  list_merchants_new_or_old['new']

        # loop for create new merchants path
        for merchant in list_merchants_new:
            # create new path
            s3.put_object(Bucket=bucket_name, Key=merchant + '/')
            print(f'-- path creation of {merchant} on bucket name {bucket_name}--')
            break
        
        
