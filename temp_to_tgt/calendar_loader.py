from library.Logger import Logger
from library.Variables import Variables
from library.Database import Database
import os

table_name = os.path.basename(__file__).split('_loader.')[0]
logger = Logger(table_name)
db = Database(logger)


tables = ['year', 'half_year', 'quarter', 'month', 'day', 'hour', 'min']

try:
    # year table
    to_table = f"""{Variables.get_value("target_database")}.d_retail_time_{tables[0]}_t"""

    query = f"""TRUNCATE TABLE {to_table};"""
    db.execute_query(query)

    query = f"""
                INSERT INTO {to_table} (ID, YEAR_KY, YEAR_START_DATE, YEAR_END_DATE, OPEN_CLOSE_CD, ROW_INSRT_TMS, ROW_UPDT_TMS)
                SELECT
                    ID,
                    YEAR_KY,
                    YEAR_START_DATE,
                    YEAR_END_DATE,
                    'O',
                    CURRENT_TIMESTAMP(6) AS ROW_INSRT_TMS,
                    CURRENT_TIMESTAMP(6) AS ROW_UPDT_TMS
                FROM {Variables.get_value("temp_database")}.tmp_{tables[0]} ;"""
    db.execute_query(query)


    # half-year table
    to_table = f"""{Variables.get_value("target_database")}.d_retail_time_{tables[1]}_t"""

    query = f"""TRUNCATE TABLE {to_table};"""
    db.execute_query(query)

    query = f"""
                    INSERT INTO {to_table} (ID, HALF_YEAR_KY, YEAR_KY, HALF_YEAR_START_DATE, HALF_YEAR_END_DATE, OPEN_CLOSE_CD, ROW_INSRT_TMS, ROW_UPDT_TMS)
                    SELECT
                        ID, 
                        ID,
                        YEAR_KY, 
                        HALF_YEAR_START_DATE, 
                        HALF_YEAR_END_DATE,
                        'O',
                        CURRENT_TIMESTAMP(6) AS ROW_INSRT_TMS,
                        CURRENT_TIMESTAMP(6) AS ROW_UPDT_TMS
                    FROM {Variables.get_value("temp_database")}.tmp_halfyear ;"""
    db.execute_query(query)

    # quarter table
    to_table = f"""{Variables.get_value("target_database")}.d_retail_time_{tables[2]}_t"""

    query = f"""TRUNCATE TABLE {to_table};"""
    db.execute_query(query)
    query = f"""
                        INSERT INTO {to_table} (ID, YEAR_KY, HALF_YEAR_KY, QUARTER_START_DATE, QUARTER_END_DATE, OPEN_CLOSE_CD, ROW_INSRT_TMS, ROW_UPDT_TMS)
                        SELECT
                            ID,
                            YEAR_KY, 
                            HALF_YEAR_KY, 
                            QUARTER_START_DATE, 
                            QUARTER_END_DATE,
                            'O',
                            CURRENT_TIMESTAMP(6) AS ROW_INSRT_TMS,
                            CURRENT_TIMESTAMP(6) AS ROW_UPDT_TMS
                        FROM {Variables.get_value("temp_database")}.tmp_{tables[2]} ;"""
    db.execute_query(query)

    # month table
    to_table = f"""{Variables.get_value("target_database")}.d_retail_time_{tables[3]}_t"""

    query = f"""TRUNCATE TABLE {to_table};"""
    db.execute_query(query)

    query = f"""
                        INSERT INTO {to_table} (ID, QUARTER_KY, YEAR_KY, HALF_YEAR_KY, MONTH_START_DATE, MONTH_END_DATE, OPEN_CLOSE_CD, ROW_INSRT_TMS, ROW_UPDT_TMS)
                        SELECT
                            ID, 
                            QUARTER_KY, 
                            YEAR_KY, 
                            HALF_YEAR_KY, 
                            MONTH_START_DATE, 
                            MONTH_END_DATE,
                            'O',
                            CURRENT_TIMESTAMP(6) AS ROW_INSRT_TMS,
                            CURRENT_TIMESTAMP(6) AS ROW_UPDT_TMS
                        FROM {Variables.get_value("temp_database")}.tmp_{tables[3]} ;"""
    db.execute_query(query)

    # day table
    to_table = f"""{Variables.get_value("target_database")}.d_retail_time_{tables[4]}_t"""

    query = f"""TRUNCATE TABLE {to_table};"""
    db.execute_query(query)

    query = f"""               
                INSERT INTO {to_table} (ID, MONTH_KY, QUARTER_KY, YEAR_KY, HALF_YEAR_KY, DAY_START_TIME, DAY_END_TIME,OPEN_CLOSE_CD, ROW_INSRT_TMS, ROW_UPDT_TMS)
                SELECT
                    ID, 
                    MONTH_KY, 
                    QUARTER_KY, 
                    YEAR_KY, 
                    HALF_YEAR_KY, 
                    DAY_START_TIME, 
                    DAY_END_TIME,
                    'O',
                    CURRENT_TIMESTAMP(6) AS ROW_INSRT_TMS,
                    CURRENT_TIMESTAMP(6) AS ROW_UPDT_TMS   
                FROM {Variables.get_value("temp_database")}.tmp_{tables[4]} ;"""
    db.execute_query(query)


    # hour table
    to_table = f"""{Variables.get_value("target_database")}.d_retail_time_{tables[5]}_t"""

    query = f"""TRUNCATE TABLE {to_table};"""
    db.execute_query(query)

    query = f"""               
                INSERT INTO {to_table} (ID)
                SELECT ID FROM {Variables.get_value("temp_database")}.tmp_{tables[5]} ;"""
    db.execute_query(query)


    # minute table
    to_table = f"""{Variables.get_value("target_database")}.d_retail_time_{tables[6]}_t"""

    query = f"""TRUNCATE TABLE {to_table};"""
    db.execute_query(query)

    query = f"""               
                INSERT INTO {to_table} (ID, HOUR_KY)
                SELECT ID,HOUR_KY FROM {Variables.get_value("temp_database")}.tmp_{tables[6]} ;"""
    db.execute_query(query)


except Exception as e:
    raise
finally:
    db.disconnect()
    db.logger.log_info("Disconnected")

