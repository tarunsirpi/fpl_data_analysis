import pandas as pd
import numpy as np

import sys
import os

from src.utils import load_object
from src.logger import logging
from src.exception import CustomException


class PredictPipeline():
  def __init__(self):
    pass

  def predict_points(self):
    try:
      logging.info("Prediction pipeline started")

      data = pd.read_csv(os.path.join("artifacts", "prediction_data.csv"))
      
      logging.info("Prediction input data read")

      preprocessor = load_object(os.path.join("artifacts", "preprossor.pkl"))
      model = load_object(os.path.join("artifacts", "model.pkl"))

      logging.info("Preprocessor and model objects loaded")

      result_data = data[['player_id', 'first_name', 'last_name', 'team name', 'oppenent team name']].copy()

      data = data.drop(columns=['first_name', 'last_name','player_id', 'fixture_id', 'team_id', 'team name','oppenent team name'])

      data['bps_last5'] = data['bps_last5'] + abs(data['bps_last5'].min())
      data['total_points_last5'] = data['total_points_last5'] + abs(data['total_points_last5'].min())

      cols_to_reduce_skew = ['minutes_played_last5', 'clean_sheets_last5', 'bps_last5', 'player_starts_last5', 'expected_goals_last5', 'expected_assists_last5', 'expected_goal_involvements_last5', 'expected_goals_conceded_last5', 'total_points_last5']
      data[cols_to_reduce_skew] = np.log(data[cols_to_reduce_skew] + 1)

      data = pd.get_dummies(data, columns=['player_type' ], dtype = 'int')

      scaled_data = pd.DataFrame(preprocessor.transform(data), columns= preprocessor.get_feature_names_out())

      logging.info("Transformed prediction data")

      total_points_predicted = model.predict(scaled_data)
      print(total_points_predicted)

      logging.info("Prediction done")

      total_points_predicted = np.round(total_points_predicted , 3)
      result_data['total points predicted'] = total_points_predicted

      result_data.to_csv(os.path.join("artifacts", "result_predicted.csv"), index= False)
      
      logging.info("Saved the prediction result as CSV file")

      print(result_data , type(result_data))
      return result_data

    except Exception as e:
      logging.error("Error while predicting")
      logging.error(CustomException(e,sys))
      raise CustomException(e,sys)


# get player predicted points (for single player only) :

  def predict_points_for_single_player(self, player_name):
    try:
      # logging.info("Prediction pipeline started")

      data = pd.read_csv(os.path.join("artifacts", "prediction_data.csv"))



      # logging.info("Prediction input data read")

      preprocessor = load_object(os.path.join("artifacts", "preprossor.pkl"))
      model = load_object(os.path.join("artifacts", "model.pkl"))

      # logging.info("Preprocessor and model objects loaded")

      result_data = data[['player_id', 'first_name', 'last_name', 'team name', 'oppenent team name']].copy()

      data = data.drop(columns=['first_name', 'last_name','player_id', 'fixture_id', 'team_id', 'team name','oppenent team name'])

      data['bps_last5'] = data['bps_last5'] + abs(data['bps_last5'].min())
      data['total_points_last5'] = data['total_points_last5'] + abs(data['total_points_last5'].min())

      cols_to_reduce_skew = ['minutes_played_last5', 'clean_sheets_last5', 'bps_last5', 'player_starts_last5', 'expected_goals_last5', 'expected_assists_last5', 'expected_goal_involvements_last5', 'expected_goals_conceded_last5', 'total_points_last5']
      data[cols_to_reduce_skew] = np.log(data[cols_to_reduce_skew] + 1)

      data = pd.get_dummies(data, columns=['player_type' ], dtype = 'int')

      scaled_data = pd.DataFrame(preprocessor.transform(data), columns= preprocessor.get_feature_names_out())

      # logging.info("Transformed prediction data")

      total_points_predicted = model.predict(scaled_data)
      print(total_points_predicted)

      # logging.info("Prediction done")

      total_points_predicted = np.round(total_points_predicted , 3)
      result_data['total_points_predicted'] = total_points_predicted


      result_data['full_name'] = result_data['first_name'] + ' ' + result_data['last_name']
      player_id = result_data[result_data['full_name'] == player_name]['player_id'].values[0]

      result_data = result_data[result_data['player_id'] == player_id]
      print("!!!"*10)
      print(result_data)

      predicted_value =  result_data['total_points_predicted'].values[0]

      # result_data.to_csv(os.path.join("artifacts", "result_predicted.csv"), index= False)
      
      # logging.info("Saved the prediction result as CSV file")

      print(result_data , type(result_data))
      return predicted_value

    except Exception as e:
      logging.error("Error while predicting")
      logging.error(CustomException(e,sys))
      raise CustomException(e,sys)





if __name__ == "__main__":
  PredictPipeline().predict_points()