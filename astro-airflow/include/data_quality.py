import pandas as pd
import hashlib
import great_expectations as ge

class DataQualityCheckSum:
    def __init__(self, local_file_path_dataframe, s3_file_path_dataframe):
        self.local_file_path_dataframe = local_file_path_dataframe
        self.s3_file_path_dataframe = s3_file_path_dataframe

    def read_local_file(self)->pd.DataFrame:
        """
        Read the local file and return a dataframe.
        """
        pass

    def read_s3_file(self)->pd.DataFrame:
        """
        Read the S3 file and return a dataframe.
        """
        pass

    @staticmethod
    def create_hash_dataframe(df)->str:
        df_bytes =  df.sort_index(axis=1).sort_values(by=df.columns.tolist()).to_csv(index=False).encode()
        df_hash = hashlib.md5(df_bytes).hexdigest()
        return df_hash


    def data_checksum(self)->bool:

        local_datfarame =  self.read_local_file(self)
        s3_dataframe =  self.read_s3_file(self)

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

    def __init__(self, path_to_dailytrans_s3_file):
        self.path_to_dailytrans_s3_file = path_to_dailytrans_s3_file

    def dataframe_from_minio_bucket(self):
        pass
    
    
    def data_schema_validation(self):
        """
        Validate the models using Great Expectations.
        """

        df =  self.dataframe_from_minio_bucket(self)
        # Check if the columns are present
        if set(MODEL_DAILYTRANS) == set(df.columns):
            return {'test_description':'Test Check Schema After Ingestion', 'success':True}
        return {'test_description':'Test Check Schema After Ingestion', 'success':False}
    

    def data_general_check(self):
        dataframe =  self.dataframe_from_minio_bucket(self)
        ge_dataframe =  ge.from_pandas(dataframe)
        # expect no null values on  transaction_date_timestamp, client_idenfication, client_idenfication, service_identification, amount_of_transaction
        for col in ['transaction_date_timestamp', 'client_idenfication', 'service_identification', 'amount_of_transaction']:
            # checkings null values

            json_response =  ge_dataframe.expect_column_values_to_not_be_null(col)
            assert json_response['success'] == True
            print(" ---- Test null values on column ", col, " : ", json_response['success'], "----")

        # expect no negative values on amount_of_transaction
        json_response =  ge_dataframe.expect_column_values_to_be_between('amount_of_transaction', min_value=0)
        assert json_response['success'] == True
        print(" ---- Test negative values on column amount_of_transaction : ", json_response['success'], "----")


        # expect ['ACCEPTED' 'REFUSED']
        
        json_response =  ge_dataframe.expect_column_values_to_be_in_set('type_of_transaction', ['ACCEPTED', 'REFUSED'])
        assert json_response['success'] == True
        print(" ---- Test values on column type_of_transaction : ", json_response['success'], "----")
            


        pass

        
    