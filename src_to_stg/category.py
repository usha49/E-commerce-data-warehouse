from library.Logger import Logger
from library.Database import Database
from library.Variables import Variables
import os

file_name = os.path.basename(__file__).split('.')[0]

 # extraction of file from src_to_stg table and create a csv and after creating csv load the csv file to staging table

try:
    logger = Logger (file_name)
    db = Database(logger)
    db.src_to_csv(file_name)
    filepath = f"{Variables.get_value('upload_path')}/{file_name}.csv"
    db.csv_to_staging(filepath, file_name)

except Exception as e:
    print(e)
finally:
    db.disconnect()