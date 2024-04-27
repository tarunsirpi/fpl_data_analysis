import pandas as pd

import sys
import os

from src.logger import logging
from src.exception import CustomException

from src.components.merging_raw_data import DataMerging
from src.components.data_injestion import DataInjestion
from src.components.data_transformation import DataTransformation
from src.components.model_trainer import ModelTrainer


if __name__ == "__main__":
  data_merging_obj = DataMerging()
  cleaned_data__path = data_merging_obj.merge_data()

  data_injestion_obj = DataInjestion()
  train_data_path , test_data_path = data_injestion_obj.initiate_data_ingestion(cleaned_data_path=cleaned_data__path)

  data_transformation_obj = DataTransformation()
  X_train, X_test, y_train, y_test, preprocessing_obj = data_transformation_obj.initaite_data_transformation(train_path=train_data_path, test_path=test_data_path)

  model_trainer_obj = ModelTrainer()
  model_trainer_obj.train_model(X_train= X_train, X_test=X_test, y_train=y_train, y_test=y_test)


  print(cleaned_data__path)
  print(train_data_path, test_data_path)
  print(X_train, X_test, y_train, y_test)