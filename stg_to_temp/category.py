from library.Logger import Logger
from library.Variables import Variables
from library.Database import Database
import os

table_name = os.path.basename(__file__).split('.')[0]
tmp_table_name = f"tmp_{table_name}"

try:
    logger =Logger(tmp_table_name)
    db=Database(logger)
    query = f"""TRUNCATE TABLE {Variables.get_value("temp_database")}.{tmp_table_name};"""
    db.execute_query(query)
    query = f"""
    INSERT INTO {Variables.get_value("temp_database")}.{tmp_table_name} (ctgry_id, ctgry_desc)
    SELECT id, category_desc
    FROM {Variables.get_value("stage_database")}.{table_name};"""
    db.execute_query(query)

except Exception as e:
    raise
finally:
    db.disconnect()
