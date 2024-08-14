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

    def fetch_teamday(self):
        log_name = "matchday"
        url = f"https://www.sofascore.com/api/v1/sport/football/scheduled-events/{self.date}"
        response = requests.get(url)
        status_code = response.status_code
        if status_code != 200:
            print(f"Error code: {status_code}. Stop jobs!")
        else:
            match_in_day = response.json()
            match_in_day = match_in_day['events']

            df_matchday = pd.json_normalize(match_in_day)
            df_matchday.rename(columns=lambda col: col.replace('.', '_'), inplace=True)
            df_matchday.rename(columns={"id": "match_id"}, inplace=True)
            df_football = df_matchday[df_matchday['tournament_category_sport_name'] == 'Football']

            df = pd.concat([df_football], ignore_index=True)
            print("Dataframe is here!!!")
            print(df)
            list_match_id = df["match_id"].astype(str).tolist()
            list_customId = df["customId"]
            self.export(log_name, df)

            # shotmap_df = self.shotmap(list_match_id)
            # if shotmap_df is not None:
            #     self.export("shotmap", shotmap_df)
            #
            # match_event_df = self.match_event(list_customId)
            # if match_event_df is not None:
            #     self.export("match_event", match_event_df)
            #
            # match_incident_df = self.match_incident(list_match_id)
            # if match_incident_df is not None:
            #     self.export("match_incident", match_incident_df)
            #
            # match_lineups_df = self.match_lineups(list_match_id)
            # if match_lineups_df is not None:
            #     self.export("match_lineups", match_lineups_df)
            #
            # match_manager_df = self.match_manager(list_match_id)
            # if match_manager_df is not None:
            #     self.export("match_manager", match_manager_df)
            #
            # match_statistics_df = self.match_statistics(list_match_id)
            # if match_statistics_df is not None:
            #     self.export("match_statistics", match_statistics_df)
            #
            # match_summary_df = self.match_summary(list_match_id)
            # if match_summary_df is not None:
            #     self.export("match_summary", match_summary_df)

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
                        event_data = event.copy()
                        event_data['match_id'] = match_id
                        event_data['status_code'] = status_code
                        shotmap_data.append(event_data)
                else:
                    shotmap_data.append({'match_id': match_id, 'status_code': status_code})
                    print(f"Failed to fetch data for match ID {match_id}. Status code: {status_code}")
            except Exception as e:
                shotmap_data.append({'match_id': match_id, 'status_code': None})
                print(f"An error occurred for match ID {match_id}: {e}")
            time.sleep(1)

        if shotmap_data:
            df_shotmap = pd.json_normalize(shotmap_data)
            df_shotmap.rename(columns=lambda col: col.replace('.', '_'), inplace=True)
            print(df_shotmap)
            return df_shotmap
        else:
            print("No data fetched.")
            return None
        self.export("shotmap", df_shotmap)

    def match_event(self,list_customId):
        event_data = []
        for customId in list_customId:
            url = f"https://www.sofascore.com/api/v1/event/{customId}/h2h/events"
            try:
                response = requests.get(url)
                status_code = response.status_code
                if status_code == 200:
                    data = response.json()
                    for event in data['events']:
                        event_copy = event.copy()
                        event_copy['customId'] = customId
                        event_copy['status_code'] = status_code
                        event_data.append(event_copy)
                else:
                    print(f"Failed to fetch data for match ID {customId}. Status code: {status_code}")
                    event_data.append({'match_id': customId, 'status_code': status_code})
            except Exception as e:
                print(f"An error occurred for match ID {customId}: {e}")
                event_data.append({'match_id': customId, 'status_code': None})
            time.sleep(1)

        if event_data:
            df_match_event = pd.json_normalize(event_data)
            df_match_event.rename(columns=lambda col: col.replace('.', '_'), inplace=True)
            print(df_match_event)
            return df_match_event
        else:
            print("No data fetched.")
            return None
        self.export("match_event", df_match_event)

    def match_incident(self,list_match_id):
        match_incident_data = []
        for match_id in list_match_id:
            url = f"https://www.sofascore.com/api/v1/event/{match_id}/incidents"
            try:
                response = requests.get(url)
                status_code = response.status_code
                if status_code == 200:
                    data = response.json()
                    for event in data['incidents']:
                        event_data = event.copy()
                        event_data['match_id'] = match_id
                        event_data['status_code'] = status_code
                        match_incident_data.append(event_data)
                else:
                    null_event_data = {'match_id': match_id, 'status_code': status_code}
                    match_incident_data.append(null_event_data)
                    print(f"Failed to fetch data for match ID {match_id}. Status code: {status_code}")
            except Exception as e:
                null_event_data = {'match_id': match_id, 'status_code': None}
                match_incident_data.append(null_event_data)
                print(f"An error occurred for match ID {match_id}: {e}")
            time.sleep(1)

        if match_incident_data:
            df_match_incident = pd.json_normalize(match_incident_data)
            df_match_incident.rename(columns=lambda col: col.replace('.', '_'), inplace=True)
            print(df_match_incident)
            return df_match_incident
        else:
            print("No data fetched.")
            return None
        self.export("match_incident", df_match_incident)

    def match_lineups(self,list_match_id):
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
                        player_info['match_id'] = match_id
                        player_info['status_code'] = status_code
                        match_lineups_data.append(player_info)
                else:
                    null_player_info = {'match_id': match_id, 'status_code': status_code}
                    match_lineups_data.append(null_player_info)
                    print(f"Failed to fetch data for match ID {match_id}. Status code: {status_code}")
            except Exception as e:
                null_player_info = {'match_id': match_id, 'status_code': None}
                match_lineups_data.append(null_player_info)
                print(f"An error occurred for match ID {match_id}: {e}")
            time.sleep(1)

        if match_lineups_data:
            df_lineups = pd.json_normalize(match_lineups_data)
            df_lineups.rename(columns=lambda col: col.replace('.', '_'), inplace=True)
            print(df_lineups)
            return df_lineups
        else:
            print("No data fetched.")
            return None
        self.export("match_lineups", df_lineups)

    def match_manager(self,list_match_id):
        match_managers_data = []
        match_managers_columns = None
        for match_id in list_match_id:
            url = f"https://www.sofascore.com/api/v1/event/{match_id}/managers"
            try:
                response = requests.get(url)
                status_code = response.status_code
                if status_code == 200:
                    data = response.json()
                    match_managers_data.append(data)
                    if not match_managers_columns:
                        match_managers_columns = list(data.keys())
                else:
                    null_data = {column: None for column in match_managers_columns} if match_managers_columns else {}
                    null_data['status_code'] = status_code
                    null_data['match_id'] = match_id
                    match_managers_data.append(null_data)
                    print(f"Failed to fetch data for match ID {match_id}, status code: {status_code}")
            except Exception as e:
                null_data = {column: None for column in match_managers_columns} if match_managers_columns else {}
                null_data['status_code'] = None
                null_data['match_id'] = match_id
                match_managers_data.append(null_data)
                print(f"An error occurred for match ID {match_id}: {e}")
            time.sleep(1)

        if match_managers_data:
            df_manager = pd.json_normalize(match_managers_data, sep='_')
            df_manager.rename(columns=lambda col: col.replace('.', '_'), inplace=True)
            print(df_manager)
            return df_manager
        else:
            print("No data fetched.")
            return None
        self.export("match_manager", df_manager)

    def match_statistics(self,list_match_id):
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
                    'status_code': None,
                    'match_id': match_id
                })
                print(f"An error occurred for match ID {match_id}: {e}")
            time.sleep(1)

        if match_statistics_data:
            df_match_statistics = pd.DataFrame(match_statistics_data)
            print(df_match_statistics)
            return df_match_statistics
        else:
            print("No data fetched.")
            return None
        self.export("match_statistics", df_match_statistics)

    def match_summary(self,list_match_id):
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
                        player_data['status_code'] = status_code
                        match_summary_data.append(player_data)
                    for player in data['bestAwayTeamPlayers']:
                        player_data = player.copy()
                        player_data['team'] = 'away'
                        player_data['match_id'] = match_id
                        player_data['status_code'] = status_code
                        match_summary_data.append(player_data)
                else:
                    match_summary_data.append({
                        'value': None, 'label': None, 'player_name': None, 'player_slug': None,
                        'player_shortName': None, 'player_position': None, 'player_jerseyNumber': None,
                        'player_userCount': None, 'player_id': None, 'player_marketValueCurrency': None,
                        'player_dateOfBirthTimestamp': None, 'player_fieldTranslations_nameTranslation_ar': None,
                        'player_fieldTranslations_shortNameTranslation_ar': None, 'team': None, 'match_id': match_id,
                        'status_code': status_code
                    })
                    print(f"Failed to fetch data for match ID {match_id}. Status code: {status_code}")
            except Exception as e:
                match_summary_data.append({
                    'value': None, 'label': None, 'player_name': None, 'player_slug': None,
                    'player_shortName': None, 'player_position': None, 'player_jerseyNumber': None,
                    'player_userCount': None, 'player_id': None, 'player_marketValueCurrency': None,
                    'player_dateOfBirthTimestamp': None, 'player_fieldTranslations_nameTranslation_ar': None,
                    'player_fieldTranslations_shortNameTranslation_ar': None, 'team': None, 'match_id': match_id,
                    'status_code': None
                })
                print(f"An error occurred for match ID {match_id}: {e}")
            time.sleep(1)

        if match_summary_data:
            df_summary = pd.json_normalize(match_summary_data, sep='_')
            df_summary.rename(columns=lambda col: col.replace('.', '_'), inplace=True)
            print(df_summary)
            return df_summary
        else:
            print("No data fetched.")
            return None
        self.export("match_summary", df_summary)

    def export(self, log_name, df):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = 'host.docker.internal'
        USER = 'postgres'
        PASSWORD = 'ducnm7'
        PORT = 5432
        DATABASE = 'postgres'

        connection_string = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
        engine = create_engine(connection_string)

        table_name = log_name
        df['ds'] = self.date
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print("Data has been written to the database.")
        return df
def start_task_callable():
    print("Start task executed!")

def end_task_callable():
    print("End task executed!")


def crawl_match_day(date):
    crawler = CrawlMatchInfo(date)
    crawler.fetch_teamday()

def crawl_match_event(date):
    crawler = CrawlMatchInfo(date)
    crawler.match_event(date)

def crawl_match_incident(date):
    crawler = CrawlMatchInfo(date)
    crawler.match_incident(date)

def crawl_match_lineups(date):
    crawler = CrawlMatchInfo(date)
    crawler.match_lineups(date)

def crawl_match_manager(date):
    crawler = CrawlMatchInfo(date)
    crawler.match_manager(date)

def crawl_match_statistics(date):
    crawler = CrawlMatchInfo(date)
    crawler.match_statistics(date)

def crawl_match_summary(date):
    crawler = CrawlMatchInfo(date)
    crawler.match_summary(date)

def crawl_shotmap(date):
    crawler = CrawlMatchInfo(date)
    crawler.shotmap(date)

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
    "ETL_SofaScore_Matchday_daily",
    default_args={
        "email": "nguyenmduc2407@gmail.com",
        "email_on_failure": True,
        "retries": 1,
        "retry_delay": timedelta(hours=1),
        'depends_on_past': False,
    },
    description="Crawl Match info in last day",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 31),
    end_date=datetime(2023, 9, 1),
    max_active_runs=2,
    tags=["ducnm7"]
)

# step 0: start dag
step_start = task_start(dag)
# Final: end dag
step_end = task_end(dag)

crawl_match_day = PythonOperator(
    task_id="crawl_data",
    python_callable=crawl_match_day,
    op_kwargs={"date": "{{ ds }}"},
    dag=dag
)

crawl_shotmap = PythonOperator(
    task_id="crawl_shotmap",
    python_callable=crawl_shotmap,
    op_kwargs={"date": "{{ ds }}"},
    dag=dag
)

crawl_shotmap = PythonOperator(
    task_id="crawl_shotmap",
    python_callable=crawl_shotmap,
    op_kwargs={"date": "{{ ds }}"},
    dag=dag
)

crawl_match_event = PythonOperator(
    task_id="crawl_match_event",
    python_callable=crawl_match_event,
    op_kwargs={"date": "{{ ds }}"},
    dag=dag
)

crawl_match_summary = PythonOperator(
    task_id="crawl_match_summary",
    python_callable=crawl_match_summary,
    op_kwargs={"date": "{{ ds }}"},
    dag=dag
)

crawl_match_lineups = PythonOperator(
    task_id="crawl_match_lineups",
    python_callable=crawl_match_lineups,
    op_kwargs={"date": "{{ ds }}"},
    dag=dag
)

crawl_match_manager = PythonOperator(
    task_id="crawl_match_manager",
    python_callable=crawl_match_manager,
    op_kwargs={"date": "{{ ds }}"},
    dag=dag
)

crawl_match_incident = PythonOperator(
    task_id="crawl_match_incident",
    python_callable=crawl_match_incident,
    op_kwargs={"date": "{{ ds }}"},
    dag=dag
)

crawl_match_statistics = PythonOperator(
    task_id="crawl_match_statistics",
    python_callable=crawl_match_statistics,
    op_kwargs={"date": "{{ ds }}"},
    dag=dag
)

(step_start >> crawl_match_day >> [crawl_shotmap, crawl_match_event, crawl_match_summary, crawl_match_lineups,
                                   crawl_match_manager, crawl_match_incident, crawl_match_statistics] >>
 step_end)
