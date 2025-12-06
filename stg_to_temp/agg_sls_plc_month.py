from library.Logger import Logger
from library.Variables import Variables
from library.Database import Database
import os


table_name = os.path.basename(__file__).split('.')[0]
tmp_table_name = f"tmp_{table_name}"
logger = Logger(tmp_table_name)
db = Database(logger)

try:
    query = f"""TRUNCATE TABLE {Variables.get_value("temp_database")}.{tmp_table_name};"""
    db.execute_query(query)

    query = f"""
                INSERT INTO {Variables.get_value("temp_database")}.{tmp_table_name}
                (PDT_KY, STORE_KY, CTGRY_KY, MONTH_KY, TOTAL_QTY, TOTAL_AMT, TOTAL_DSCNT)
                SELECT 
                    PDT_KY, 
                    STORE_KY, 
                    CTGRY_KY, 
                    MONTH(S.DT_KY) AS MONTH_KY, 
                    SUM(S.QTY), 
                    SUM(S.AMT), 
                    SUM(S.DSCNT)               
                FROM {Variables.get_value("temp_database")}.TMP_SALES AS S
                JOIN {Variables.get_value("temp_database")}.TMP_PRODUCT AS P
                ON S.PDT_KY = P.PDT_ID
                GROUP BY S.PDT_KY, S.STORE_KY, MONTH_KY, P.CTGRY_KY
                ORDER BY PDT_KY;"""
    db.execute_query(query)

except Exception as e:
    raise
finally:
    db.disconnect()
    db.logger.log_info("Disconnected")
