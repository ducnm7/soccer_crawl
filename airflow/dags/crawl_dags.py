import pandas as pd
import requests
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

class crawl_match_info:
    def __init__(self, to_date, headers):
        self.to_date = to_date
        self.headers = headers

    def fetch_data(self):
        url = f"https://www.sofascore.com/api/v1/sport/football/scheduled-events/{self.to_date}"
        response = requests.get(url, headers=self.headers)
        status_code = response.status_code

        if status_code != 200:  # Compare status code as an integer
            print(f"Error code: {status_code}. Stop jobs!")
        else:
            match_in_day = response.json()
            match_in_day = match_in_day['events']

            # Normalize the nested JSON data
            df = pd.json_normalize(match_in_day)
            df.rename(columns=lambda col: col.replace('.', '_'), inplace=True)
            return df

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

def task_start(dag):
    operator = PythonOperator(
        task_id='start',
        dag=dag
    )
    return operator

def task_end(dag):
    operator = PythonOperator(
        task_id='end',
        dag=dag
    )
    return operator

dag = DAG(
    "ETL_SofaScore_matchday_daily",
    default_args={
        "email": "nguyenmduc2407@gmail.com",
        "email_on_failure": True,
        "retries": 1,
        "retry_delay": timedelta(hours=1),
        'depends_on_past': False,
    },
    description="Crawl Match info in last day",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024,5,9),
    tags=["ducnm7"]
)

# step 0: start dag
step_start = task_start(dag)
# Final: end dag
step_end = task_end(dag)

crawl_data = PythonOperator(
    task_id="crawl_data",
    python_callable=crawl_match_info,
    op_kwargs={"to_date": "{{ ds }}"},
    dag=dag
)

step_start >> crawl_data >> step_end