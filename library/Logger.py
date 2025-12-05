# create a log file and write whatever you need here
import datetime     #while creating your own log file
import os.path
import logging   #if you want to use a library of log file
from library.Variables import Variables as Variables

class Logger:
    def __init__(self, file_name):
        current_ts = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        self.file_name= f"{file_name}_{current_ts}.log"
        self.log_path= os.path.join(Variables.get_value('log_path'),self.file_name)

        os.makedirs(os.path.dirname(self.log_path), exist_ok= True)
        self.logger = self.set_logger()

    def set_logger(self):
        logger = logging.getLogger(self.file_name)
        logger.setLevel('DEBUG')
        #Create a file Handler
        file_handler = logging.FileHandler(self.log_path)
        file_handler.setLevel(logging.DEBUG)

        #Create a formatter and set it for the handler
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        #Add the handler to the logger
        if not logger.handlers:
            logger.addHandler(file_handler)

        return logger

    def log_info(self,msg):
        self.logger.info(msg)

    def log_error(self,msg):
        self.logger.error(msg)
