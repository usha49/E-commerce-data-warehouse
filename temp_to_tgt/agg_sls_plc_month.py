from library.Logger import Logger
from library.Variables import Variables
from library.Database import Database
import os

table_name = os.path.basename(__file__).split('.')[0]
tmp_table_name = f"tmp_{table_name}"
tgt_table_name = "F_RETAIL_AGG_SLS_PLC_MONTH_T"

logger = Logger(tmp_table_name)
db = Database(logger)

try:
    query = f"""TRUNCATE TABLE {Variables.get_value("target_database")}.{tgt_table_name};"""
    db.execute_query(query)

    query = f"""
                INSERT INTO {Variables.get_value("target_database")}.{tgt_table_name}
                (PDT_KY, LOCN_KY, MONTH_KY, TOTAL_QTY, TOTAL_AMT, TOTAL_DSCNT, ROW_INSRT_TMS, ROW_UPDT_TMS)
                SELECT 
                    PDT_KY, 
                    STORE_KY, 
                    MONTH_KY,
                    TOTAL_QTY,
                    TOTAL_AMT,
                    TOTAL_DSCNT,
                    CURRENT_TIMESTAMP(6) AS ROW_INSRT_TMS,
                    CURRENT_TIMESTAMP(6) AS ROW_UPDT_TMS
                FROM {Variables.get_value("temp_database")}.{tmp_table_name};"""
    db.execute_query(query)

except Exception as e:
    raise
finally:
    db.disconnect()
    db.logger.log_info("Disconnected")
