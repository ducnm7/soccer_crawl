import time

import pandas as pd
import requests
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

class CrawlMatchInfo:
    def __init__(self, date):
        self.date = date

    def fetch_matchday(self):
        log_name = "matchday"
        url = f"https://www.sofascore.com/api/v1/sport/football/scheduled-events/{self.date}"
        response = requests.get(url)
        status_code = response.status_code

        if status_code != 200:
            print(f"Error code: {status_code}. Stop jobs!")
            return None
        else:
            match_in_day = response.json()
            match_in_day = match_in_day['events']

            # Normalize the nested JSON data
            df_matchday = pd.json_normalize(match_in_day)
            df_matchday.rename(columns=lambda col: col.replace('.', '_'), inplace=True)
            df_matchday.rename(columns={"id": "match_id"}, inplace=True)
            df = df_matchday[df_matchday['tournament_category_sport_name'] == 'Football']
            print("Dataframe is here!!!")
            print(df)
            list_match_id = df["match_id"].astype(str).tolist()
            list_customId = df["customID"]
            self.export(log_name, df)

            # Fetch and export shotmap data
            shotmap_df = self.shotmap(list_match_id)
            if shotmap_df is not None:
                self.export("shotmap", shotmap_df)

            match_event_df = self.match_event(list_customId)
            if match_event_df is not None:
                self.export("match_event", match_event_df)

            match_incident_df = self.match_incident(list_match_id)
            if match_incident_df is not None:
                self.export("match_incident", match_incident_df)

            match_lineups_df = self.match_lineups(list_match_id)
            if match_lineups_df is not None:
                self.export("match_lineups", match_lineups_df)

            match_manager_df = self.match_manager(list_match_id)
            if match_manager_df is not None:
                self.export("match_manager", match_manager_df)

    def shotmap(self, list_match_id):
        shotmap_data = []

        for match_id in list_match_id:
            url = f"https://www.sofascore.com/api/v1/event/"+match_id+"/shotmap"
            try:
                response = requests.get(url)
                status_code = response.status_code
                if status_code == 200:
                    data = response.json()
                    for event in data['shotmap']:
                        event_data = event.copy()  # Create a copy of the event dictionary
                        event_data['match_id'] = match_id  # Add match_id to the copy
                        shotmap_data.append(event_data)  # Append the modified event data
                else:
                    print(f"Failed to fetch data for match ID {match_id}. Status code: {status_code}")
            except Exception as e:
                print(f"An error occurred for match ID {match_id}: {e}")
            time.sleep(1)

        if shotmap_data:
            # Normalize the nested JSON data
            df_shotmap = pd.json_normalize(shotmap_data)
            df_shotmap.rename(columns=lambda col: col.replace('.', '_'), inplace=True)
            return df_shotmap
        else:
            return None

    def match_event(self, list_customId):
        event_data = []

        for customId in list_customId:
            url = f"https://www.sofascore.com/api/v1/event/"+customId+"/h2h/events"
            try:
                response = requests.get(url)
                status_code = response.status_code
                if status_code == 200:
                    data = response.json()
                    for event in data['events']:
                        event_data = event.copy()  # Create a copy of the event dictionary
                        event_data.rename(columns={"id": "match_id"}, inplace=True)
                        event_data.append(event_data)  # Append the modified event data
                else:
                    print(f"Failed to fetch data for match ID {customId}. Status code: {status_code}")
            except Exception as e:
                print(f"An error occurred for match ID {customId}: {e}")
            time.sleep(1)

        if event_data:
            # Normalize the nested JSON data
            df_match_event = pd.json_normalize(event_data)
            df_match_event.rename(columns=lambda col: col.replace('.', '_'), inplace=True)
            return df_match_event
        else:
            return None

    def match_incident(self, list_match_id):
        match_incident_data = []

        for match_id in list_match_id:
            url = f"https://www.sofascore.com/api/v1/event/"+match_id+"/incidents"
            try:
                response = requests.get(url)
                status_code = response.status_code
                if status_code == 200:
                    data = response.json()
                    for event in data['incidents']:
                        event_data = event.copy()  # Create a copy of the event dictionary
                        event_data['match_id'] = match_id  # Add match_id to the copy
                        match_incident_data.append(event_data)  # Append the modified event data
                else:
                    print(f"Failed to fetch data for match ID {match_id}. Status code: {status_code}")
            except Exception as e:
                print(f"An error occurred for match ID {match_id}: {e}")
            time.sleep(1)

        if match_incident_data:
            # Normalize the nested JSON data
            df_match_incident = pd.json_normalize(match_incident_data)
            df_match_incident.rename(columns=lambda col: col.replace('.', '_'), inplace=True)
            return df_match_incident
        else:
            return None

    def match_lineups(self, list_match_id):
        match_lineups_data = []
        for match_id in list_match_id:
            url = f"https://www.sofascore.com/api/v1/event/{match_id}/lineups"
            try:
                response = requests.get(url)
                status_code = response.status_code
                if status_code == 200:
                    data = response.json()
                    for player in data["home"]["players"]:
                        player_info = player["player"]
                        stats = player["statistics"]
                        player_info.update(stats)
                        player_info.update({
                            "shirtNumber": player["shirtNumber"],
                            "jerseyNumber": player["jerseyNumber"],
                            "position": player["position"],
                            "substitute": player["substitute"]
                        })
                        match_lineups_data.append(player_info)

                    df_lineups = pd.DataFrame(match_lineups_data)
                    df_lineups['match_id'] = match_id  # Add match_id to the copy
            except Exception as e:
                print(f"An error occurred: {e}")

    def match_manager(self, list_match_id):
        match_managers_data = []
        for match_id in list_match_id:
            url = f"https://www.sofascore.com/api/v1/event/{match_id}/managers"
            try:
                response = requests.get(url)
                status_code = response.status_code
                if status_code == 200:
                    data = response.json()
                    match_managers_data.append(data)
                    df_manager = pd.json_normalize(match_managers_data, sep='_')
                    df_manager['match_id'] = match_id
                else:
                    print(f"Failed to fetch data for match ID {match_id}, status code: {status_code}")
            except Exception as e:
                print(f"An error occurred for match ID {match_id}: {e}")

    def match_statistics(self, list_match_id):
        match_statistics_data = []

        # for match_id in list_match_id:
        for match_id in list_match_id:
            url = f"https://www.sofascore.com/api/v1/event/{match_id}/statistics"
            try:
                response = requests.get(url)
                status_code = response.status_code
                if status_code == 200:
                    data = response.json()
                    # Assuming 'statistics' is a list containing match statistics
                    for event in data['statistics']:
                        for group in event['groups']:
                            for stat_item in group['statisticsItems']:
                                # Append extracted data to the match_statistics_data list
                                match_statistics_data.append({
                                    'period': event['period'],
                                    'groupName': group['groupName'],
                                    'name': stat_item['name'],
                                    'home': stat_item['home'],
                                    'away': stat_item['away'],
                                    'compareCode': stat_item['compareCode'],
                                    'statisticsType': stat_item['statisticsType'],
                                    'valueType': stat_item['valueType'],
                                    'homeValue': stat_item['homeValue'],
                                    'awayValue': stat_item['awayValue'],
                                    'renderType': stat_item['renderType'],
                                    'key': stat_item['key']
                                })
                    df_match_statistics = pd.json_normalize(match_statistics_data)
                    df_match_statistics['match_id'] = match_id
                else:
                    # Print an error message if the request fails
                    print(f"Failed to fetch data for match ID {match_id}. Status code: {status_code}")
            except requests.exceptions.RequestException as e:
                # Handle specific exceptions related to requests
                print(f"An error occurred for match ID {match_id}: {e}")

            # Convert match statistics data to a DataFrame if data was fetched successfully
            # if df_match_statistics:
            #     df_match_statistics.rename(columns=lambda col: col.replace('.', '_'), inplace=True)

        else:
            print("No data fetched.")



    def export(self, log_name, df):
        # Replace the placeholders with your PostgreSQL credentials and database details
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = 'host.docker.internal'  # Change to your database endpoint
        USER = 'postgres'
        PASSWORD = 'ducnm7'
        PORT = 5432
        DATABASE = 'postgres'

        # Connection string
        connection_string = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
        engine = create_engine(connection_string)

        # Export the DataFrame to a PostgreSQL table
        table_name = log_name
        df['ds'] = self.date
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print("Data has been written to the database.")
        return df
def start_task_callable():
    print("Start task executed!")

def end_task_callable():
    print("End task executed!")

def crawl_data_callable(date):
    crawler = CrawlMatchInfo(date)
    crawler.fetch_matchday()

def task_start(dag):
    operator = PythonOperator(
        task_id='start',
        python_callable=start_task_callable,
        dag=dag
    )
    return operator

def task_end(dag):
    operator = PythonOperator(
        task_id='end',
        python_callable=end_task_callable,
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
    start_date=datetime(2024, 5, 9),
    end_date=datetime(2024, 5, 10),
    tags=["ducnm7"]
)

# step 0: start dag
step_start = task_start(dag)
# Final: end dag
step_end = task_end(dag)

crawl_data = PythonOperator(
    task_id="crawl_data",
    python_callable=crawl_data_callable,
    op_kwargs={"date": "{{ ds }}"},
    dag=dag
)

step_start >> crawl_data >> step_end
