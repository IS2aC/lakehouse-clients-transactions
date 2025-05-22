import pandas as pd
import hashlib
import great_expectations as ge
from datetime import datetime
import boto3
from io import StringIO


# Configuration MinIO
MINIO_END_POINT = "http://minio:9000"  
MINIO_ACCESS_KEY = "64MmRGXPfPqrRcb10oFJ"
MINIO_SECRET_KEY = "OjOM5BfW0EnNbQvrm8PX5VuSoNognOOOTuJIfvrW"


class DataQualityCheckSum:
    def __init__(self, local_file_path_dataframe, s3_file_path_dataframe, bucket_name):
        self.local_file_path_dataframe = local_file_path_dataframe
        self.s3_file_path_dataframe = s3_file_path_dataframe
        self.bucket_name = bucket_name

    def read_local_file(self)->pd.DataFrame:
        """
        Read the local file and return a dataframe.
        """
        df = pd.read_csv(self.local_file_path_dataframe)
        return df

    def read_s3_file(self)->pd.DataFrame:
        """
        Read the S3 file and return a dataframe.
        """
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_END_POINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name="us-east-1",  # important
        )


        # read object
        response = s3.get_object(Bucket=self.bucket_name, Key=self.s3_file_path_dataframe)
        csv_data = response['Body'].read().decode('utf-8')  # Si encodé en UTF-8

        # Chargement dans une DataFrame
        df = pd.read_csv(StringIO(csv_data))
        
        return df

    @staticmethod
    def create_hash_dataframe(df)->str:
        df_bytes =  df.sort_index(axis=1).sort_values(by=df.columns.tolist()).to_csv(index=False).encode()
        df_hash = hashlib.md5(df_bytes).hexdigest()
        return df_hash


    def data_checksum(self)->bool:

        local_datfarame =  self.read_local_file()
        s3_dataframe =  self.read_s3_file()

        # hashing
        local_dataframe_hash = self.create_hash_dataframe(local_datfarame)
        s3_dataframe_hash =  self.create_hash_dataframe(s3_dataframe)

        if local_dataframe_hash == s3_dataframe_hash:
            return {'test_description':'Test CheckSum After Ingestion', 'success':True}
        return {'test_description':'Test CheckSum After Ingestion', 'success':False}
    

MODEL_DAILYTRANS = ['transaction_date_timestamp', 'client_idenfication',
       'service_identification', 'client_domain', 'amount_currency',
       'type_of_transaction', 'country_of_transaction', 'log_message',
       'amount_of_transaction']

class DataQualityVerifications:

    def __init__(self, path_to_dailytrans_local_file=None):
        self.path_to_dailytrans_local_file = path_to_dailytrans_local_file
        if self.path_to_dailytrans_local_file is None:
            print("⚠️ No file path provided. Please provide a valid file path.")
            # raise ValueError("File path cannot be None.")

    def read_dataframe(self)->pd.DataFrame:
        """
        Read the local file and return a dataframe.
        """
        df = pd.read_csv(self.path_to_dailytrans_local_file)
        return df
    
    def data_schema_validation(self):
        """
        Validate the models using Great Expectations.
        """

        df =  self.read_dataframe()
        # Check if the columns are present
        if set(MODEL_DAILYTRANS) == set(df.columns):
            return {'test_description':'Test Check Schema After Ingestion', 'success':True}
        return {'test_description':'Test Check Schema After Ingestion', 'success':False}
    

    def checks_null_detections(self):
        dataframe =  self.read_dataframe()
        ge_dataframe =  ge.from_pandas(dataframe)
        # expect no null values on  transaction_date_timestamp, client_idenfication, client_idenfication, service_identification, amount_of_transaction
        for col in ['transaction_date_timestamp', 'client_idenfication', 'service_identification', 'amount_of_transaction']:
            # checkings null values

            json_response =  ge_dataframe.expect_column_values_to_not_be_null(col)
            assert json_response['success'] == True
            print(" ---- Test null values on column ", col, " : ", json_response['success'], "----")
        


    def checks_negative_value_over_amount_transaction(self):
        dataframe =  self.read_dataframe()
        ge_dataframe =  ge.from_pandas(dataframe)
        # expect no negative values on amount_of_transaction
        json_response =  ge_dataframe.expect_column_values_to_be_between('amount_of_transaction', min_value=0)
        assert json_response['success'] == True
        print(" ---- Test negative values on column amount_of_transaction : ", json_response['success'], "----")

        

    def checks_over_type_transactions(self):
        # expect ['ACCEPTED' 'REFUSED']
        dataframe =  self.read_dataframe()
        ge_dataframe =  ge.from_pandas(dataframe)
        json_response =  ge_dataframe.expect_column_values_to_be_in_set('type_of_transaction', ['ACCEPTED', 'REFUSED'])
        assert json_response['success'] == True
        print(" ---- Test values on column type_of_transaction : ", json_response['success'], "----")
            

    def checks_over_transaction_date_timestamp(self):
        dataframe =  self.read_dataframe()
        # checking on date transaction_date_timestamp
        dataframe['transaction_date_timestamp'] = pd.to_datetime(dataframe['transaction_date_timestamp'])
        dataframe['date'] =  [datetime.strftime(i, '%Y-%m-%d') for i in dataframe['transaction_date_timestamp']]
        path_file = self.path_to_dailytrans_local_file.split('/')[-1].split('.')[0].split('_')[1]
    
        assert str(dataframe['date'].unique()[0]) == str(path_file)
        print(" ---- Test date on column transaction_date_timestamp : ", str(dataframe['date'].unique()[0]) == str(path_file), "----")