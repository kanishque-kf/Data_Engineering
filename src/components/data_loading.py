import os
import sys
import pandas as pd
import boto3
import json

from dataclasses import dataclass

from src.exceptions import CustomException
from src.logger import logging

from src.components.data_transformation import DataTransformation

import src.config as con



@dataclass
class DataLoadingConfig:
    log_data_path: str = os.path.join('artifacts','log_data.csv')
    song_data_path: str = os.path.join('artifacts','song_data.csv')
    

class DataLoading:
    def __init__(self) -> None:
        self.loading_config = DataLoadingConfig()
        
    def read_s3_to_dataframe(self,bucket_name, prefix, file_extension=".json"):
        
        # set up AWS credentials
        aws_access_key_id = con.aws_access_key_id
        aws_secret_access_key = con.aws_secret_access_key
        region_name = con.region_name

        

        # set up S3 client
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, 
                  aws_secret_access_key=aws_secret_access_key,
                  region_name=region_name)
        
        logging.info("Made connection with S3")
        
        obj_list = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)['Contents']
        dfs = []
        for obj in obj_list:
            if obj['Key'].endswith(file_extension):
                file_content = s3.get_object(Bucket=bucket_name, Key=obj['Key'])['Body'].read().decode('utf-8')
                json_data = [json.loads(x) for x in file_content.strip().split('\n')]
                df = pd.json_normalize(json_data)
                dfs.append(df)
        result_df = pd.concat(dfs)
        
        return result_df
        
    def initiate_data_loader(self):
        
        # specify bucket name and JSON file key
        bucket_name = con.bucket_name
        song_prefix = 'data/song_data/'
        log_prefix = 'data/log-data/'
        
        try:
            
            os.makedirs(os.path.dirname(self.loading_config.log_data_path),exist_ok = True)
            os.makedirs(os.path.dirname(self.loading_config.song_data_path),exist_ok = True)
            
            song_df = self.read_s3_to_dataframe(bucket_name,song_prefix)
            song_df.to_csv(self.loading_config.song_data_path, index = False,header=True)
            log_df = self.read_s3_to_dataframe(bucket_name,log_prefix)
            log_df.to_csv(self.loading_config.log_data_path, index = False,header=True)
            
            logging.info("Log data and Song data saved to artifacts")
            
            return(
                self.loading_config.log_data_path,
                self.loading_config.song_data_path
            )
            
        except Exception as e:
            raise CustomException(e,sys)

if __name__=="__main__":
    obj = DataLoading()
    log_data,song_data = obj.initiate_data_loader()
    print(log_data,song_data)
 
    transformer = DataTransformation()
    transformer.initiate_transformation(log_data,song_data)
    