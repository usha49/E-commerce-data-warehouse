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



# def csv_to_staging(db,csv_filename,table_name):
#
#     try:
#         query = f"""
#         LOAD DATA LOCAL INFILE '{csv_filename}'
#         INTO TABLE {Variables.get_value("stage_database")}.{table_name}
#         FIELDS TERMINATED BY ','  -- CSV delimiter
#         ENCLOSED BY '"'           -- Enclose values in double quotes (if applicable)
#         LINES TERMINATED BY '\n'  -- Line delimiter
#         IGNORE 1 LINES           -- Skip the header row;
#         """
#         db.logger.log_info(f"Query: {query}")
#         db.execute_query(query)
#         db.logger.log_info(f"Successfully executed query")
#         db.commit_query()
#         db.logger.log_info(f"Successfully committed query")
#         db.logger.log_info("Data loaded successfully into the staging table.")
#     except Exception as e:
#         db.logger.log_error(f"Error loading data to stage_table {e}")

# csv_filepath = "D:/py-projects/library/outputs/output_df.csv"
#
# csv_to_staging(db,csv_filepath)



# file_name pathayera csv create garnu paryo


# def extract_to_csv():
#     ...
# no insert:
# use loadinfile library file to load at the target table
#
#
# OLTP_usha--> src --> Product table
# OLAP_usha --> TARGET--> STG/TEMP/TARGET TABLE


