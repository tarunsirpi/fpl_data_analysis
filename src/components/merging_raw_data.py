import pandas as pd

import os
import sys
import warnings


from src.logger import logging
from src.exception import CustomException
from src.utils import get_historical_data, get_difficulty_and_is_home_team

warnings.filterwarnings('ignore') 

class DataMergingConfig():
  def __init__(self):
    self.player_gw_data_path = os.path.join("Data-Engineering", "data", "individual_player_data.csv")
    self.player_data_path = os.path.join("Data-Engineering", "data", "player_transformed_data.csv")
    self.fixture_data_path = os.path.join("Data-Engineering", "data", "fixture_transformed_data.csv")
    self.merged_data_path = os.path.join("artifacts", "merged_data.csv")
    self.cleaned_data_path = os.path.join("artifacts", "cleaned_data.csv")

class DataMerging():
  def __init__(self):
    self.data_merging_config = DataMergingConfig()

  def merge_data(self):
    try:
      player_gw_data = pd.read_csv(self.data_merging_config.player_gw_data_path)
      player_data = pd.read_csv(self.data_merging_config.player_data_path)
      fixture_data = pd.read_csv(self.data_merging_config.fixture_data_path)
      logging.info("reading raw data for merging")
      

      index_greater_than_90 = player_gw_data[player_gw_data['minutes_played']>90].index
      player_gw_data.loc[index_greater_than_90, 'minutes_played'] = 90

      df = player_gw_data[["player_id", "total_points", "fixture_id", "gameWeek"]].copy()
      logging.info("created new dataframe for merging data")

      historical_data_cols = ['minutes_played_last5','clean_sheets_last5', 'bps_last5', 'player_starts_last5', 'expected_goals_last5', 'expected_assists_last5', 'expected_goal_involvements_last5', 'expected_goals_conceded_last5', 'total_points_last5',]
      df[historical_data_cols] = 0
      print(df.tail())
      print("shape of df ", df.shape)
  
      for index in player_gw_data.index:
        df.loc[index, historical_data_cols] = get_historical_data(index = index, player_gw_data = player_gw_data, df = df)


      logging.info("historical data calculated")
      print("shape of df ", df.shape)

      df2 = df.copy()
      player_copy = player_data[['first_name', 'last_name', 'player_id', 'team_id', 'player_type']].copy()
      print(player_copy.head(2))
      
      df3 = pd.merge(df2, player_copy, how='left', on='player_id')
      print(df3.tail())
      logging.info("merged player data with GW data")

      df3['difficulty'] = 0
      df3['is_home_team'] = 0

      for index in df3.index:
        df3.loc[index, ['difficulty', 'is_home_team']] = get_difficulty_and_is_home_team(index = index, data = df3, fixture_data = fixture_data)
        
      logging.info("merged fixture data to GW data")

      difficulty_median = df3['difficulty'].median()
      for index in df3.index:
        if df3.loc[index, 'difficulty'] == -1 :
          df3.loc[index, 'difficulty'] = difficulty_median

      df3.to_csv(self.data_merging_config.merged_data_path, index=False)
      
      logging.info("merged data is saved to artifacts")
          
      df4 = df3.copy()
      df4.dropna(axis=0, inplace = True)
      df4.to_csv(self.data_merging_config.cleaned_data_path, index=False)

      logging.info("Null values are dropped and the cleaned data is saved to artifacts as csv file")


    except Exception as e :
      logging.error(CustomException(e, sys))
      raise CustomException(e, sys)

if __name__ == "__main__":
  DataMerging().merge_data()

