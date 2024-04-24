import os
import sys
from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder,StandardScaler
from src.components.data_injestion import DataInjestion

from src.exception import CustomException
from src.logger import logging

# from src.utils import save_model


class DataTransformationConfig():
  def __init__(self):
    self.preprocessor_obj_file = os.path.join("artifacts", "preprossor.pkl")


class DataTransformation():
  def __init__(self):
    self.data_transformation_config = DataTransformationConfig()

  def get_data_transformation_object(self):
    try:
      pass

    except Exception as e:
        logging.info("Error in getting the Data Transformation object")
        logging.error(CustomException(e,sys))
        raise CustomException(e,sys)




  def initaite_data_transformation(self, train_path, test_path):
    try:
      train_df = pd.read_csv(train_path)
      test_df = pd.read_csv(test_path)

      logging.info('Read train and test data completed')
      logging.info("Shape of train data : {}  and shape of test data : {}".format(train_df.shape, test_df.shape))

      train_df = train_df[train_df['total_points'] < 9]
      train_df = train_df[train_df['total_points'] >= 0 ]

      test_df = test_df[test_df['total_points'] < 9]
      test_df = test_df[test_df['total_points'] >= 0 ]

      logging.info("Shape of train data : {}  and shape of test data : {} after removing outliers".format(train_df.shape, test_df.shape))


      X_train = train_df.copy()
      X_train = train_df.drop(columns=['total_points'])
      y_train = train_df['total_points'].copy()

      X_test = test_df.copy()
      X_test = test_df.drop(columns=['total_points'])
      y_test = test_df['total_points'].copy()

      logging.info("input and target features of train and test data split")
      #######################################################################
      X_train = X_train.drop(columns=['first_name', 'last_name','player_id', 'fixture_id', 'team_id', ])
      X_test = X_test.drop(columns=['first_name', 'last_name','player_id', 'fixture_id', 'team_id', ])


      X_train['bps_last5'] = X_train['bps_last5'] + abs(X_train['bps_last5'].min())
      X_train['total_points_last5'] = X_train['total_points_last5'] + abs(X_train['total_points_last5'].min())

      X_test['bps_last5'] = X_test['bps_last5'] + abs(X_test['bps_last5'].min())
      X_test['total_points_last5'] = X_test['total_points_last5'] + abs(X_test['total_points_last5'].min())


      cols_to_reduce_skew = ['minutes_played_last5', 'clean_sheets_last5', 'bps_last5', 'player_starts_last5', 'expected_goals_last5', 'expected_assists_last5', 'expected_goal_involvements_last5', 'expected_goals_conceded_last5', 'total_points_last5']
      X_train[cols_to_reduce_skew] = np.log(X_train[cols_to_reduce_skew] + 1)
      X_test[cols_to_reduce_skew] = np.log(X_test[cols_to_reduce_skew] + 1)


      X_train = pd.get_dummies(X_train, columns=['player_type' ], dtype = 'int')
      X_test = pd.get_dummies(X_test, columns=['player_type' ], dtype = 'int')
      X_test.to_csv("X_train_3.csv", index = False)

      X_train = X_train.drop_duplicates()
      X_test = X_test.drop_duplicates()


      logging.info("Shape of input train data : {}  and shape of input test data : {} after transformation".format(X_train.shape, X_test.shape))

      #########################################################################

      # print(train_df.tail(), test_df.shape)
      
    except Exception as e:
      logging.info("Exception occured in the initiate_datatransformation")
      raise CustomException(e,sys)
    


if __name__ == "__main__":
  # data_injestion = DataInjestion()
  # train_data_path , test_data_path = data_injestion.initiate_data_ingestion()
  train = "artifacts/train.csv"
  test = "artifacts/test.csv"
  print(train)
  transformation_obj = DataTransformation()
  transformation_obj.initaite_data_transformation(train_path=train, test_path=test)



