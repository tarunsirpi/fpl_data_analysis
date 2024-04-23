import pandas as pd
from sklearn.model_selection import train_test_split

import os
import sys
from dataclasses import dataclass

from src.logger import logging
from src.exception import CustomException


class DataInjestionConfig():
  def __init__(self):
    self.train_data_path = os.path.join("artifacts", "train.csv")
    self.test_data_path = os.path.join("artifacts", "test.csv")
    self.cleaned_data_path = os.path.join("artifacts", "cleaned_data.csv")
  

class DataInjestion():
  def __init__(self):
    self.data_injestion_config = DataInjestionConfig()

  def initiate_data_ingestion(self):
    logging.info("Starting data ingestion")

    try:
      df  = pd.read_csv(self.data_injestion_config.cleaned_data_path)
      logging.info("Merged data reading completed")

      train_set, test_set = train_test_split(df,test_size=0.1, random_state=10)
      logging.info("train and test data split complete")

      train_set.to_csv(self.data_injestion_config.train_data_path, index = False, header = True)
      test_set.to_csv(self.data_injestion_config.test_data_path, index = False, header = True)
      logging.info("data ingestion complete")

      return(self.data_injestion_config.train_data_path, self.data_injestion_config.test_data_path)


    except Exception as e :
      logging.error(CustomException(e, sys))
      raise CustomException(e, sys)


if __name__ == "__main__" :
  temp = DataInjestionConfig()
  print(temp.test_data_path)
  print(os.getcwd())

  test = DataInjestion()
  test.initiate_data_ingestion()
