# import boto3
# import pandas as pd
# from io import StringIO  # Pour CSV texte. Utilise BytesIO pour fichiers binaires.

# # Configuration MinIO
# minio_endpoint = "http://localhost:9000"  # ou https://minio.example.com
# access_key = "64MmRGXPfPqrRcb10oFJ"
# secret_key = "OjOM5BfW0EnNbQvrm8PX5VuSoNognOOOTuJIfvrW"
# bucket_name = "dailytrans" 
# object_key = "trx_2025-05-16.csv"  # 

# # Création du client boto3 pour MinIO
# s3 = boto3.client(
#     "s3",
#     endpoint_url=minio_endpoint,
#     aws_access_key_id=access_key,
#     aws_secret_access_key=secret_key,
#     region_name="us-east-1",  # requis mais peut être fictif
# )

# # Lecture de l'objet
# response = s3.get_object(Bucket=bucket_name, Key=object_key)
# csv_data = response['Body'].read().decode('utf-8')  # Si encodé en UTF-8

# # Chargement dans une DataFrame
# df = pd.read_csv(StringIO(csv_data))

# # Affichage pour vérification
# print(df.iloc[0])

import hashlib
import boto3
import pandas as pd
from io import StringIO

# Configuration MinIO
MINIO_END_POINT = "http://localhost:9000"  
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

if __name__ == "__main__":
    dqcs = DataQualityCheckSum(
        local_file_path_dataframe = '/home/isaac/lakehouse-clients-transactions/astro-airflow/include/dailytrans/trx_2025-05-16.csv',
        s3_file_path_dataframe = 'trx_2025-05-16.csv',
        bucket_name = 'dailytrans'
    )

    print(dqcs.data_checksum())