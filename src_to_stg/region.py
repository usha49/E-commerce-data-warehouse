from library.Logger import Logger
from library.Database import Database
from library.Variables import Variables
import os

file_name = os.path.basename(__file__).split('.')[0]
# print(file_name)
    # extraction of file from src_to_stg table and create a csv and after creating csv load the csv file to staging table
# logger = Logger(file_name)
# db = Database(logger)
try:
    db= Database(file_name)
    tablename = file_name
    db.ext_to_file(tablename)
    filepath = f"{Variables.get_value('upload_path')}/{tablename}.csv"
    db.csv_to_staging(filepath, tablename)

except Exception as e:
    print(e)
finally:
    db.disconnect()

