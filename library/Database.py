import mysql.connector
from library.Variables import Variables
from library.Logger import Logger
import os
import pandas as pd


class Database:
    def __init__(self, logger: Logger):
        """Initialize the database connection and use the provided logger."""
        self.logger = logger  # Uses an existing Logger instance
        self.connection_params = {
            'user': Variables.get_value('user'),
            'host': Variables.get_value('host'),
            'database': Variables.get_value('SRC_DB'),
            'password': Variables.get_value('password'),
            'port' : Variables.get_value('port'),
        }
        self.con = mysql.connector.connect(**self.connection_params)
        self.cur = self.con.cursor()

    def execute_query(self, query):
        try:
            self.logger.log_info(f"Query: {query}")
            self.cur.execute(query)
            if query.strip().upper().startswith(("INSERT", "UPDATE", "DELETE", "LOAD DATA")):
                self.con.commit()
        except Exception as e:
            print(f"[ERROR in query]: {e}")

    def fetchall(self):
        data = self.cur.fetchall()
        columns = [desc[0] for desc in self.cur.description]  # Column names from cursor description
        df = pd.DataFrame(data, columns=columns)
        print(df)
        return df

    def commit_query(self):
        self.con.commit()

    def disconnect(self):
        self.cur.close()
        self.con.close()
        self.logger.log_info("Database connection closed successfully")

    def src_to_csv(self, table_name):
        query = f"""SELECT * FROM {Variables.get_value('SRC_DB')}.{table_name}"""
        self.execute_query(query)

        # Fetch data
        data = self.fetchall()
        upload_path = f"{Variables.get_value('upload_file_path')}/{table_name}.csv"
        os.makedirs(os.path.dirname(upload_path), exist_ok=True)
        data.to_csv(upload_path, index=False)
        self.logger.log_info(f"Successfully exported {table_name}.csv")

    def csv_to_staging(self, csv_filepath, table_name):
        try:
            query = f"""
            LOAD DATA INFILE '{csv_filepath}'
            INTO TABLE {Variables.get_value('stage_database')}.{table_name}
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES;
            """
            self.logger.log_info(f"Query: {query}")
            self.execute_query(query)
            self.commit_query()
            self.logger.log_info("Data loaded successfully into the staging table.")
        except Exception as e:
            self.logger.log_error(f"Error loading data to stage_table {e}")
