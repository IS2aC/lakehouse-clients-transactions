import pandas as pd
import hashlib

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
    


class DataQualityWithGreatExpectations:
    model_list = []
    pass