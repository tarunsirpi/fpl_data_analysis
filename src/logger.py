import logging
from datetime import datetime
import os


LOG_FILE = f"{datetime.now().strftime('%m_%d_%Y_%H-%M-%S')}.log"
log_path = os.path.join(os.getcwd(),"logs")

os.makedirs(log_path,exist_ok=True)
LOG_FILE_PATH=os.path.join(log_path,LOG_FILE)

logging.basicConfig(filename=LOG_FILE_PATH,level=logging.INFO,format="[%(asctime)s] %(lineno)d %(name)s -%(levelname)s %(message)s")

