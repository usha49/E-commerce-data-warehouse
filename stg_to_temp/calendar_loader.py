from library.Logger import Logger
from library.Variables import Variables
from library.Database import Database
import os

table_name = os.path.basename(__file__).split('_loader.')[0]
tmp_table_name = f"dim_{table_name}"
logger = Logger(tmp_table_name)
db = Database(logger)
tables = ['year', 'halfyear', 'quarter', 'month', 'day', 'hour', 'min']


try:
    calendar = f"""{Variables.get_value("temp_database")}.{tmp_table_name}"""

    # year table
    to_table = f"""{Variables.get_value("temp_database")}.tmp_{tables[0]}"""

    query = f"""TRUNCATE TABLE {to_table};"""
    db.execute_query(query)

    query = f"""
                INSERT INTO {to_table} (ID, YEAR_KY, YEAR_START_DATE, YEAR_END_DATE)
                SELECT
                    ROW_NUMBER() OVER (ORDER BY year) AS ID,
                    year AS YEAR_KY,
                    MIN(date) AS YEAR_START_DATE,
                    MAX(date) AS YEAR_END_DATE
                FROM
                    {calendar}
                GROUP BY
                    year;"""
    db.execute_query(query)


    # half-year table
    to_table = f"""{Variables.get_value("temp_database")}.tmp_{tables[1]}"""

    query = f"""TRUNCATE TABLE {to_table};"""
    db.execute_query(query)

    query = f"""
                INSERT INTO {to_table} (ID, YEAR_KY, HALF_YEAR_START_DATE, HALF_YEAR_END_DATE)
                SELECT
                    ROW_NUMBER() OVER (ORDER BY year, half_year) AS ID,
                    year AS YEAR_KY,
                    MIN(date) AS HALF_YEAR_START_DATE,
                    MAX(date) AS HALF_YEAR_END_DATE
                FROM (
                    SELECT
                        year,
                        CASE
                            WHEN MONTH(date) BETWEEN 1 AND 6 THEN 1
                            ELSE 2
                        END AS half_year,
                        date
                    FROM
                        {calendar}
                ) AS half_year_data
                GROUP BY
                    year, half_year;"""
    db.execute_query(query)


    # quarter table
    to_table = f"""{Variables.get_value("temp_database")}.tmp_{tables[2]}"""

    query = f"""TRUNCATE TABLE {to_table};"""
    db.execute_query(query)

    query = f"""               
                INSERT INTO {to_table} (ID, YEAR_KY, HALF_YEAR_KY, QUARTER_START_DATE, QUARTER_END_DATE)
                SELECT
                    ROW_NUMBER() OVER (ORDER BY year, quarter) AS ID,
                    year AS YEAR_KY,
                    CASE
                        WHEN quarter BETWEEN 1 AND 2 THEN 1
                        ELSE 2
                    END AS HALF_YEAR_KY,
                    MIN(date) AS QUARTER_START_DATE,
                    MAX(date) AS QUARTER_END_DATE
                FROM
                    {calendar}
                GROUP BY
                    year, quarter;"""
    db.execute_query(query)


    # month table
    to_table = f"""{Variables.get_value("temp_database")}.tmp_{tables[3]}"""

    query = f"""TRUNCATE TABLE {to_table};"""
    db.execute_query(query)

    query = f"""               
                INSERT INTO {to_table} (ID, QUARTER_KY, HALF_YEAR_KY, YEAR_KY, MONTH_START_DATE, MONTH_END_DATE)
                SELECT
                    ROW_NUMBER() OVER (ORDER BY year, month) AS ID,
                    quarter as QUARTER_KY,
                    CASE
                        WHEN month BETWEEN 1 AND 6 THEN 1
                        ELSE 2
                    END AS HALF_YEAR_KY,
                    year AS YEAR_KY,
                    MIN(date) AS MONTH_START_DATE,
                    MAX(date) AS MONTH_END_DATE
                FROM
                    {calendar}
                GROUP BY
                    year, quarter, month;"""
    db.execute_query(query)


    # day table
    to_table = f"""{Variables.get_value("temp_database")}.tmp_{tables[4]}"""

    query = f"""TRUNCATE TABLE {to_table};"""
    db.execute_query(query)

    query = f"""               
                INSERT INTO {to_table} (ID, MONTH_KY, QUARTER_KY, YEAR_KY, HALF_YEAR_KY, DAY_START_TIME, DAY_END_TIME)
                SELECT
                    ROW_NUMBER() OVER (ORDER BY date) AS ID,
                    month AS MONTH_KY,
                    quarter AS QUARTER_KY,
                    year AS YEAR_KY,
                    CASE
                        WHEN quarter BETWEEN 1 AND 2 THEN 1
                        ELSE 2
                    END AS HALF_YEAR_KY,
                    DATE_FORMAT(date, '%Y-%m-%d 00:00:00') AS DAY_START_TIME,
                    DATE_FORMAT(date, '%Y-%m-%d 23:59:59') AS DAY_END_TIME
                FROM
                    {calendar};"""
    db.execute_query(query)


    # hour table
    to_table = f"""{Variables.get_value("temp_database")}.tmp_{tables[5]}"""

    query = f"""TRUNCATE TABLE {to_table};"""
    db.execute_query(query)

    query = f"""               
                INSERT INTO {to_table} (ID)
                WITH RECURSIVE Hours AS (
                    SELECT 0 AS HOUR
                    UNION ALL
                    SELECT HOUR + 1
                    FROM Hours
                    WHERE HOUR < 23
                )
                SELECT HOUR FROM Hours;"""
    db.execute_query(query)


    # minute table
    to_table = f"""{Variables.get_value("temp_database")}.tmp_{tables[6]}"""

    query = f"""TRUNCATE TABLE {to_table};"""
    db.execute_query(query)

    query = f"""               
                INSERT INTO {to_table} (ID, HOUR_KY)
                WITH RECURSIVE 
                Hours AS (
                    SELECT 0 AS HOUR
                    UNION ALL
                    SELECT HOUR + 1
                    FROM Hours
                    WHERE HOUR < 23
                ),
                Minutes AS (
                    SELECT 0 AS MINUTE
                    UNION ALL
                    SELECT MINUTE + 1
                    FROM Minutes
                    WHERE MINUTE < 59
                )
                SELECT
                    MINUTE AS ID,
                    HOUR AS HOUR_KY
                FROM
                    Hours
                JOIN
                    Minutes
                ORDER BY
                    HOUR, MINUTE;"""
    db.execute_query(query)


except Exception as e:
    raise
finally:
    db.disconnect()
    db.logger.log_info("Disconnected")

