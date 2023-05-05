import os
import sys
import pandas as pd
import boto3
import json

from dataclasses import dataclass

from src.exceptions import CustomException
from src.logger import logging


import src.config as con

@dataclass
class DataTransformConfig:
    song_path: str = os.path.join('artifacts','song.csv')
    artists_path: str = os.path.join('artifacts','artist.csv')
    user_path: str = os.path.join('artifacts','user.csv')
    time_path: str = os.path.join('artifacts','time.csv')
    #songplays_path: str = os.path.join('artifacts','songplays.csv')

class DataTransformation():
    def __init__(self) -> None:
        self.transformation_config = DataTransformConfig()
        
    def initiate_transformation(self,log_data_path,song_data_path):
        
        try:
            song_data = pd.read_csv(song_data_path)
            log_data = pd.read_csv(log_data_path)
            song_df = song_data[['song_id','title','artist_id','year','duration']]  
            artist_df = song_data[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]      
            user_df = log_data[['userId','firstName','lastName','gender','level']]
            time_df = log_data[['sessionId','ts']]
            time_df['ts'] = pd.to_datetime(time_df['ts'])
            time_df['hour'] = time_df['ts'].dt.hour
            time_df['day'] =time_df['ts'].dt.day
            time_df['week'] = time_df['ts'].dt.isocalendar().week
            time_df['month'] = time_df['ts'].dt.month
            time_df['year'] = time_df['ts'].dt.year
            time_df['weekday'] = time_df['ts'].dt.day_name()
            time_df = time_df.drop('ts', axis=1)
            
            os.makedirs(os.path.dirname(self.transformation_config.song_path),exist_ok = True)
            os.makedirs(os.path.dirname(self.transformation_config.artists_path),exist_ok = True)
            os.makedirs(os.path.dirname(self.transformation_config.user_path),exist_ok = True)
            os.makedirs(os.path.dirname(self.transformation_config.time_path),exist_ok = True)
            #os.makedirs(os.path.dirname(self.transformation_config.songplays_path),exist_ok = True)
            
            song_df.to_csv(self.transformation_config.song_path, index = False,header=True)
            artist_df.to_csv(self.transformation_config.artists_path, index = False,header=True)
            user_df.to_csv(self.transformation_config.user_path, index = False,header=True)
            time_df.to_csv(self.transformation_config.time_path, index = False,header=True)
            
            logging.info("Dimension Tables saved")
            
            return(
                self.transformation_config.song_path,
                self.transformation_config.artists_path,
                self.transformation_config.user_path,
                self.transformation_config.time_path
            )
        
        except Exception as e:
            raise CustomException(e,sys)
        
