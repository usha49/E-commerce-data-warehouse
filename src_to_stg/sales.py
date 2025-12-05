from library.Logger import Logger
from library.Database import Database
from library.Variables import Variables
import os
import pandas as pd


file_name = os.path.basename(__file__).split('.')[0]
table_name = file_name

try:
    logger = Logger(file_name)
    db = Database(logger)
      #to bring from source to csv
    query = f"""select * from {Variables.get_value('SRC_DB')}.{table_name}"""
    db.execute_query(query)

    # Fetch data (assumes self.fetch() returns a list of tuples)
    data_df = db.fetchall()

    if 'CUSTOMER_ID' in data_df.columns:
        data_df['CUSTOMER_ID'] = data_df['CUSTOMER_ID'].apply(lambda x: '' if pd.isna(x) else int(x))
    # If x is NaN, return ''(an empty string)
    # Otherwise, return int(x)(convert x to an integer).

    upload_path = f"{Variables.get_value('upload_path')}/{table_name}.csv"
    os.makedirs(os.path.dirname(upload_path), exist_ok=True)
    data_df.to_csv(upload_path, index=False)
    db.logger.log_info(f"Successfully exported {table_name}.csv")

    # db.load_to_stage_table(file_name)
    query = f"""
                LOAD DATA INFILE '{upload_path}'
                INTO TABLE {Variables.get_value('stage_database')}.{table_name}
                FIELDS TERMINATED BY ',' 
                ENCLOSED BY '"' 
                LINES TERMINATED BY '\n'
                IGNORE 1 ROWS
                (ID, STORE_ID, PRODUCT_ID, @CUSTOMER_ID, TRANSACTION_TIME, QUANTITY, AMOUNT, DISCOUNT)
                SET 
                    CUSTOMER_ID = NULLIF(@CUSTOMER_ID, '');
                """
    db.execute_query(query)
    db.logger.log_info(f"Successfully imported {table_name}.csv")

except Exception as e:
    raise
finally:
    db.disconnect()
 #next class ma data milayera dashboard samma sabai lyare aau