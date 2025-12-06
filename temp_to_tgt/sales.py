from library.Logger import Logger
from library.Variables import Variables
from library.Database import Database
import os

table_name = os.path.basename(__file__).split('.')[0]
tmp_table_name = f"tmp_{table_name}"
tgt_table_name = "F_RETAIL_SLS_T"

try:
    logger = Logger(tgt_table_name)
    db = Database(logger)
    query = f"""TRUNCATE TABLE {Variables.get_value("target_database")}.{tgt_table_name};"""
    db.execute_query(query)
    query = f"""
    INSERT INTO {Variables.get_value("target_database")}.{tgt_table_name} (SLS_ID, LOCN_KY,DT_KY, PDT_KY, CUSTOMER_KY, TRANSACTION_TIME, QTY, AMT, DSCNT, OPEN_CLOSE_CD, ROW_INSRT_TMS, ROW_UPDT_TMS)
    SELECT
        SLS_ID,
        STORE_KY,
        DT_KY,
        PDT_KY, 
        CUSTOMER_KY, 
        TRANSACTION_TIME, 
        QTY, 
        AMT, 
        DSCNT,
        'O',
        CURRENT_TIMESTAMP(6) AS ROW_INSRT_TMS,
        CURRENT_TIMESTAMP(6) AS ROW_UPDT_TMS
    FROM {Variables.get_value("temp_database")}.{tmp_table_name};"""
    db.execute_query(query)

except Exception as e:
    raise
finally:
    db.disconnect()