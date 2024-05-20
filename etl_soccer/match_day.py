import pandas as pd
import requests
from airflow.core.etl_base import EtlBase
from airflow.core.conf_base import ConfBase
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
from airflow.etl_soccer.cookies import cookies, headers

class match_day_Conf(ConfBase):
    def __init__(self, in_file, in_ds, in_params):
        super(match_day_Conf, self).__init__("match_day", in_file, in_ds, in_params)


class match_day_Etl(EtlBase):
    def read_file(self):
        spark = self.spark
        in_path = self.conf.in_path
        ds = self.conf.ds
        in_file = self.conf.in_file

        print("------------------------------ BEGIN ETL {0}".format(in_file))

    response = requests.get("https://www.sofascore.com/api/v1/sport/football/scheduled-events/"+ ds +"", headers=headers)
    status_code = response.status_code
    if status_code != "200":
        print("Error code: "+status_code+". Stop jobs!")
    else:
        match_in_day = response.json()
        match_in_day = match_in_day['events']

        # Normalize the nested JSON data
        df = pd.json_normalize(match_in_day)

        # Rename columns
        df.rename(columns=lambda col: col.replace('.', '_'), inplace=True)

        # Replace the placeholders with your PostgreSQL credentials and database details
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = 'localhost'  # Change to your database endpoint
        USER = 'postgres'
        PASSWORD = 'ducnm7'
        PORT = 5432
        DATABASE = 'postgres'

        # Connection string
        connection_string = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
        engine = create_engine(connection_string)

        # Step 3: Export the DataFrame to a PostgreSQL table
        table_name = 'matchday'
        df.to_sql(table_name, engine, if_exists='append', index=False)

        print(f"DataFrame exported to PostgreSQL table '{table_name}' successfully.")


