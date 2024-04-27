import pandas as pd
import numpy as np
import sys
import os

from src.exception import CustomException
from src.logger import logging

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


from src.utils import save_object, evaluate_model
from src.components.data_injestion import DataInjestion
from src.components.data_transformation import DataTransformation


class ModelTrainerConfig():
  def __init__(self):
    self.trained_model_file_path = os.path.join('artifacts','model.pkl')


class ModelTrainer():
  def __init__(self):
    self.model_trainer_config = ModelTrainerConfig()

  def train_model(self, X_train, X_test, y_train, y_test):
    try:
      model = RandomForestRegressor(n_estimators=100, min_samples_split=10, min_samples_leaf=4, max_features='sqrt', max_depth=50, bootstrap=True, random_state=10, verbose=2, )
      # model = RandomForestRegressor(n_estimators=100, min_samples_split=10, min_samples_leaf=4, max_features='sqrt')
      """# best parameters from hyper parameter tuning (refer data_analysis_notebook)
      # {'n_estimators': 100,
      # 'min_samples_split': 10,
      # 'min_samples_leaf': 4,
      # 'max_features': 'sqrt',
      # 'max_depth': 50,
      # 'bootstrap': True}
      """
      
      logging.info("model initiated")

      model.fit(X_train, y_train)

      logging.info("model trained")
      logging.info(model)

      print(model)

      mae, mse, r2, max_pred_value = evaluate_model(model, X_test, y_test)
      print("\n Mean_Absolute_Error: {}\n Mean_Squuared_Error: {},\n r2 score: {},\n maximum predicted value: {}".format(mae, mse, r2, max_pred_value))
      logging.info("\n Mean_Absolute_Errorr: {}\n Mean_Squuared_Error: {},\n r2 score: {},\n maximum predicted value: {}".format(mae, mse, r2, max_pred_value))

      save_object(self.model_trainer_config.trained_model_file_path, model)
      logging.info("saved the trained model as a pickle file")


    except Exception as e:
      logging.error("Exception occured in model training")
      logging.error(CustomException(e, sys))
      raise CustomException(e, sys)


if __name__ == "__main__":
  train = "artifacts/train.csv"
  test = "artifacts/test.csv"
  print(train)
  transformation_obj = DataTransformation()
  X_train, X_test, y_train, y_test, preprocessing_obj = transformation_obj.initaite_data_transformation(train_path=train, test_path=test)

  model_trainer_obj = ModelTrainer()
  model_trainer_obj.train_model(X_train= X_train, X_test=X_test, y_train=y_train, y_test=y_test)


