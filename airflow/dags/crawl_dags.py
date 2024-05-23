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
        else:
            match_in_day = response.json()
            match_in_day = match_in_day['events']

            # Normalize the nested JSON data
            df_matchday = pd.json_normalize(match_in_day)
            df_matchday.rename(columns=lambda col: col.replace('.', '_'), inplace=True)
            df_matchday.rename(columns={"id": "match_id"}, inplace=True)
            df_football = df_matchday[df_matchday['tournament_category_sport_name'] == 'Football']

            # Print the combined DataFrame
            df = pd.concat([df_football], ignore_index=True)
            print("Dataframe is here!!!")
            print(df)
            list_match_id = df["match_id"].astype(str).tolist()
            list_customId = df["customId"]
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

            match_statistics_df = self.match_statistics(list_match_id)
            if match_statistics_df is not None:
                self.export("match_statistics", match_statistics_df)

            match_summary_df = self.match_summary(list_match_id)
            if match_summary_df is not None:
                self.export("match_statistics", match_summary_df)

    def shotmap(self, list_match_id):
        shotmap_data = []

        for match_id in list_match_id:
            url = f"https://www.sofascore.com/api/v1/event/{match_id}/shotmap"
            try:
                response = requests.get(url)
                status_code = response.status_code
                if status_code == 200:
                    data = response.json()
                    for event in data['shotmap']:
                        event_data = event.copy()  # Create a copy of the event dictionary
                        event_data['match_id'] = match_id  # Add match_id to the copy
                        event_data['status_code'] = status_code  # Add status_code to the copy
                        shotmap_data.append(event_data)  # Append the modified event data
                else:
                    # Append a row with all null values and the status_code
                    shotmap_data.append({
                        'match_id': match_id,
                        'status_code': status_code,
                        **{key: None for key in data['shotmap'][0].keys()}
                    })
                    print(f"Failed to fetch data for match ID {match_id}. Status code: {status_code}")
            except Exception as e:
                # Append a row with all null values and the status_code
                shotmap_data.append({
                    'match_id': match_id,
                    'status_code': None,
                    **{key: None for key in data['shotmap'][0].keys()}
                })
                print(f"An error occurred for match ID {match_id}: {e}")
            time.sleep(1)

        if shotmap_data:
            # Normalize the nested JSON data
            df_shotmap = pd.json_normalize(shotmap_data)
            df_shotmap.rename(columns=lambda col: col.replace('.', '_'), inplace=True)
            df_shotmap
        else:
            print("No data fetched.")

    def match_event(self, list_customId):
        event_data = []

        for customId in list_customId:
            url = f"https://www.sofascore.com/api/v1/event/{customId}/h2h/events"
            try:
                response = requests.get(url)
                status_code = response.status_code
                if status_code == 200:
                    data = response.json()
                    for event in data['events']:
                        event_copy = event.copy()  # Create a copy of the event dictionary
                        event_copy['customId'] = customId  # Add match_id to the event data
                        event_copy['status_code'] = status_code  # Add status_code to the event data
                        event_data.append(event_copy)  # Append the modified event data
                else:
                    print(f"Failed to fetch data for match ID {customId}. Status code: {status_code}")
                    event_data.append({'match_id': customId, 'status_code': status_code})
            except Exception as e:
                print(f"An error occurred for match ID {customId}: {e}")
                event_data.append({'match_id': customId, 'status_code': None})
            time.sleep(1)

        if event_data:
            # Normalize the nested JSON data
            df_match_event = pd.json_normalize(event_data)
            df_match_event.rename(columns=lambda col: col.replace('.', '_'), inplace=True)
            df_match_event
        else:
            print("No data fetched.")

    def match_incident(self, list_match_id):
        match_incident_data = []

        for match_id in list_match_id:
            url = f"https://www.sofascore.com/api/v1/event/{match_id}/incidents"
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
                    # Add null values for all columns and add status_code and match_id columns
                    null_event_data = {'match_id': match_id, 'status_code': status_code}
                    for key in match_incident_data[0].keys():
                        if key not in ['match_id', 'status_code']:
                            null_event_data[key] = None
                    match_incident_data.append(null_event_data)
            except Exception as e:
                print(f"An error occurred for match ID {match_id}: {e}")
            time.sleep(1)

        if match_incident_data:
            # Normalize the nested JSON data
            df_match_incident = pd.json_normalize(match_incident_data)
            df_match_incident.rename(columns=lambda col: col.replace('.', '_'), inplace=True)
            df_match_incident
        else:
            print("No data fetched.")

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
                        player_info['match_id'] = match_id  # Add match_id to each player_info
                        match_lineups_data.append(player_info)
                else:
                    # For status_code != 200, add a row with all null values and add status_code and match_id
                    null_player_info = {key: None for key in match_lineups_data[0].keys()} if match_lineups_data else {}
                    null_player_info['status_code'] = status_code
                    null_player_info['match_id'] = match_id
                    match_lineups_data.append(null_player_info)
            except Exception as e:
                print(f"An error occurred for match ID {match_id}: {e}")

        df_lineups = pd.DataFrame(match_lineups_data)
        df_lineups


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
                    # Extract column names if not already extracted
                    if not match_managers_columns:
                        match_managers_columns = list(data.keys())
                else:
                    print(f"Failed to fetch data for match ID {match_id}, status code: {status_code}")
                    # Create a dictionary with null values and append it to match_managers_data
                    null_data = {column: None for column in match_managers_columns}  # assuming match_managers_columns contains all column names
                    null_data['status_code'] = status_code
                    null_data['match_id'] = match_id
                    match_managers_data.append(null_data)
            except Exception as e:
                print(f"An error occurred for match ID {match_id}: {e}")

        # Normalize the data and create DataFrame
        df_manager = pd.json_normalize(match_managers_data, sep='_')
        df_manager

    def match_statistics(self, list_match_id):
        match_statistics_data = []

        for match_id in list_match_id:
            url = f"https://www.sofascore.com/api/v1/event/{match_id}/statistics"
            try:
                response = requests.get(url)
                status_code = response.status_code
                if status_code == 200:
                    data = response.json()
                    for event in data['statistics']:
                        for group in event['groups']:
                            for stat_item in group['statisticsItems']:
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
                                    'key': stat_item['key'],
                                    'status_code': status_code,
                                    'match_id': match_id
                                })
                else:
                    # Add null values for all columns
                    match_statistics_data.append({
                        'period': None,
                        'groupName': None,
                        'name': None,
                        'home': None,
                        'away': None,
                        'compareCode': None,
                        'statisticsType': None,
                        'valueType': None,
                        'homeValue': None,
                        'awayValue': None,
                        'renderType': None,
                        'key': None,
                        'status_code': status_code,
                        'match_id': match_id
                    })
                    print(f"Failed to fetch data for match ID {match_id}. Status code: {status_code}")
            except requests.exceptions.RequestException as e:
                print(f"An error occurred for match ID {match_id}: {e}")

        df_match_statistics = pd.DataFrame(match_statistics_data)
        df_match_statistics

    def match_summary(self, list_match_id):
        match_summary_data = []
        for match_id in list_match_id:
            url = f"https://www.sofascore.com/api/v1/event/{match_id}/best-players/summary"
            try:
                response = requests.get(url)
                status_code = response.status_code
                if status_code == 200:
                    data = response.json()
                    for player in data['bestHomeTeamPlayers']:
                        player_data = player.copy()
                        player_data['team'] = 'home'
                        player_data['match_id'] = match_id
                        match_summary_data.append(player_data)
                    for player in data['bestAwayTeamPlayers']:
                        player_data = player.copy()
                        player_data['team'] = 'away'
                        player_data['match_id'] = match_id
                        match_summary_data.append(player_data)
                else:
                    # Add a row with all null values except for status_code and match_id
                    match_summary_data.append({
                        'value': None, 'label': None, 'player_name': None, 'player_slug': None, 'player_shortName': None,
                        'player_position': None, 'player_jerseyNumber': None, 'player_userCount': None, 'player_id': None,
                        'player_marketValueCurrency': None, 'player_dateOfBirthTimestamp': None,
                        'player_fieldTranslations_nameTranslation_ar': None,
                        'player_fieldTranslations_shortNameTranslation_ar': None, 'team': None, 'match_id': match_id,
                        'status_code': status_code
                    })
            except Exception as e:
                print(f"An error occurred for match ID {match_id}: {e}")
                # Add a row with all null values except for status_code and match_id in case of an exception
                match_summary_data.append({
                    'value': None, 'label': None, 'player_name': None, 'player_slug': None, 'player_shortName': None,
                    'player_position': None, 'player_jerseyNumber': None, 'player_userCount': None, 'player_id': None,
                    'player_marketValueCurrency': None, 'player_dateOfBirthTimestamp': None,
                    'player_fieldTranslations_nameTranslation_ar': None,
                    'player_fieldTranslations_shortNameTranslation_ar': None, 'team': None, 'match_id': match_id,
                    'status_code': status_code
                })

        if match_summary_data:
            df_summary = pd.json_normalize(match_summary_data, sep='_')
            df_summary.rename(columns=lambda col: col.replace('.', '_'), inplace=True)
            df_summary
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
