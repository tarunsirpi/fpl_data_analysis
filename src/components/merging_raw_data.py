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
    self.team_data_path = os.path.join("Data-Engineering", "data", "team_transformed_data.csv")

    self.merged_data_path = os.path.join("artifacts", "merged_data.csv")
    self.cleaned_data_path = os.path.join("artifacts", "cleaned_data.csv")

    self.prediction_data_path = os.path.join("artifacts", "prediction_data.csv")

class DataMerging():
  def __init__(self):
    self.data_merging_config = DataMergingConfig()

  def merge_data(self):
    try:
      player_gw_data = pd.read_csv(self.data_merging_config.player_gw_data_path)
      player_data = pd.read_csv(self.data_merging_config.player_data_path)
      fixture_data = pd.read_csv(self.data_merging_config.fixture_data_path)
      logging.info("reading raw data for merging")
      

      player_gw_data = player_gw_data[player_gw_data['gameWeek'] < 38]
      

      index_greater_than_90 = player_gw_data[player_gw_data['minutes_played']>90].index
      player_gw_data.loc[index_greater_than_90, 'minutes_played'] = 90

      df = player_gw_data[["player_id", "total_points", "fixture_id", "gameWeek"]].copy()
      logging.info("created new dataframe for merging data")

      historical_data_cols = ['minutes_played_last5','clean_sheets_last5', 'bps_last5', 'player_starts_last5', 'expected_goals_last5', 'expected_assists_last5', 'expected_goal_involvements_last5', 'expected_goals_conceded_last5', 'total_points_last5',]
      df[historical_data_cols] = 0
      print(df.tail())
      print("shape of df ", df.shape)
  
      for index in player_gw_data.index:
        df.loc[index, historical_data_cols] = get_historical_data(index = index, gw_data = player_gw_data, search_df = player_gw_data)


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
      ###############
      df3.to_csv('merged8.csv')
      ################
      logging.info("merged data is saved to artifacts")
          
      df4 = df3.copy()
      df4.dropna(axis=0, inplace = True)
      df4.to_csv(self.data_merging_config.cleaned_data_path, index=False)

      logging.info("Null values are dropped and the cleaned data is saved to artifacts as csv file")

      return self.data_merging_config.cleaned_data_path

    except Exception as e :
      logging.error("Error occured while merging raw data")
      logging.error(CustomException(e, sys))
      raise CustomException(e, sys)
    
  def create_prediction_data(self):
    try:
      player_gw_data = pd.read_csv(self.data_merging_config.player_gw_data_path)
      player_data = pd.read_csv(self.data_merging_config.player_data_path)
      fixture_data = pd.read_csv(self.data_merging_config.fixture_data_path)

      team_data = pd.read_csv(self.data_merging_config.team_data_path)

      unique_player_id_list = list(player_gw_data['player_id'].unique())
      print(unique_player_id_list, type(unique_player_id_list), len(unique_player_id_list))

      max_gameweek = player_gw_data['gameWeek'].max()
      current_gameweek = max_gameweek + 1
      print("max gameweek data available for : {}".format(max_gameweek))

      current_gw_fixture_data = fixture_data[fixture_data['gameWeek'] == current_gameweek]
      print(current_gw_fixture_data)

      
      print(player_data[player_data['player_id'] == 434]['team_id'])
      print(int(player_data[player_data['player_id'] == 434]['team_id']))
      print(type(int(player_data[player_data['player_id'] == 434]['team_id'])))

      #################################

      prediction_df = pd.DataFrame(columns=["player_id", "fixture_id", "gameWeek"])

      for player_id in unique_player_id_list:
        team_id = int(player_data[player_data['player_id'] == player_id]['team_id'])
        # print(player_id, team_id)

        for index in current_gw_fixture_data.index:
          home_and_away_team_id = [current_gw_fixture_data.loc[index]['home_team'], current_gw_fixture_data.loc[index]['away_team']]

          if team_id in home_and_away_team_id :
            # print(current_gw_fixture_data.loc[index])

            fixture_id = current_gw_fixture_data.loc[index]['match_id']
            # print(fixture_id, type(fixture_id))
            # print(len(prediction_df), prediction_df.shape)

            new_row_to_add = [player_id, fixture_id, current_gameweek]
            prediction_df.loc[len(prediction_df)] = new_row_to_add

            # print(len(prediction_df), prediction_df.shape)
            
      print(prediction_df)
      print(prediction_df.columns)
      ######

      historical_data_cols = ['minutes_played_last5','clean_sheets_last5', 'bps_last5', 'player_starts_last5', 'expected_goals_last5', 'expected_assists_last5', 'expected_goal_involvements_last5', 'expected_goals_conceded_last5', 'total_points_last5',]
      prediction_df[historical_data_cols] = 0

      for index in prediction_df.index:
        prediction_df.loc[index, historical_data_cols] = get_historical_data(index = index, gw_data = player_gw_data, search_df = prediction_df)

      #########
      prediction_df2 = prediction_df.copy()
      player_copy = player_data[['first_name', 'last_name', 'player_id', 'team_id', 'player_type']].copy()
      print(player_copy.head(2))

      prediction_df3 = pd.merge(prediction_df2, player_copy, how='left', on='player_id')

      prediction_df3['difficulty'] = 0
      prediction_df3['is_home_team'] = 0

      for index in prediction_df3.index:
        prediction_df3.loc[index, ['difficulty', 'is_home_team']] = get_difficulty_and_is_home_team(index = index, data = prediction_df3, fixture_data = fixture_data)
      
      difficulty_median = prediction_df3['difficulty'].median()
      for index in prediction_df3.index:
        if prediction_df3.loc[index, 'difficulty'] == -1 :
          prediction_df3.loc[index, 'difficulty'] = difficulty_median

      #######
      prediction_df3['team name'] = 0
      prediction_df3['oppenent team name'] = 0

      for index in prediction_df3.index:
        team_id = prediction_df3.loc[index, 'team_id']
        fixture_id = prediction_df3.loc[index, 'fixture_id']

        prediction_df3.loc[index, 'team name'] = team_data[team_data['team_id'] == team_id]['team_name'].values[0]

        print(team_id, team_data[team_data['team_id'] == team_id]['team_name'].values[0])
        print(team_data[team_data['team_id'] == team_id])

        selected_fixture = fixture_data[fixture_data['match_id'] == fixture_id]

        if team_id == selected_fixture['home_team'].values[0] :
          opponent_team_id = selected_fixture['away_team'].values[0]
          prediction_df3.loc[index, 'oppenent team name'] = team_data[team_data['team_id'] == opponent_team_id]['team_name'].values[0]

        elif team_id == selected_fixture['away_team'].values[0]:
          opponent_team_id = selected_fixture['home_team'].values[0]
          prediction_df3.loc[index, 'oppenent team name'] = team_data[team_data['team_id'] == opponent_team_id]['team_name'].values[0]

        else:
          prediction_df3.loc[index, 'oppenent team name'] = 'NA'

      #######
      print(prediction_df3.isnull().sum())

      prediction_df3.fillna(0, inplace= True)
      prediction_df3.to_csv(self.data_merging_config.prediction_data_path, index=False)
      ################################
      return self.data_merging_config.prediction_data_path

    except Exception as e:
      logging.error("Error occured while creating data for prediction")
      logging.error(CustomException(e, sys))
      raise CustomException(e, sys)

if __name__ == "__main__":
  # DataMerging().merge_data()
  merge_obj = DataMerging()
  prediction_data_path = merge_obj.create_prediction_data()



  print(prediction_data_path)

