import json
import datetime
import pendulum

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator

from utils.minify_dag_helpers import task_start, task_end, task_check_hdfs_exists
from utils.general_notification import notify_failed, notify_retry, notify_success


local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")


def create_etl_dag(d_id):
    """
    Accepts a kwargs parameters dict and
    overrides default args to create a DAG object
    Returns: DAG() Object
    """
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime.datetime(2023, 9, 24, 0, 0, tzinfo=local_tz),
        'end_date': datetime.datetime(2023, 9, 25, 0, 0, tzinfo=local_tz),
        'email': ['nguyenmduc2407@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=15),
        'on_failure_callback': notify_failed,
        'on_retry_callback': notify_retry,
        'on_success_callback': notify_success,
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
        # 'log_source': 'ingame',
        # 'game_code': 'gn_016',
    }

    return DAG(
        dag_id=d_id,
        default_args=default_args,
        schedule_interval="30 8 * * *",
        catchup=True
    )

#
# product_id = "gn_016"
# root_path_data = "/ge/gamelogs/gn_016"
# root_path_data_ingame = "/ge/gamelogs/gn_016/ingame"

#recharge doesnot have data
sla_time = datetime.timedelta(minutes=120)

dag_id = "ETL_SPARK_DATASCORE_DAILY"
dag = create_etl_dag(dag_id)
globals()[dag_id] = dag

# step 0: start dag
step_start = task_start(dag)

step_end = task_end(dag)

mapping_file = {
#
}

# in_path_template_SUCCESS = '{0}/%Y-%m-%d/_SUCCESS'.format(root_path_data_ingame)
# step_check_exists_SUCCESS = task_check_hdfs_exists(dag, product_id, '_SUCCESS', in_path_template_SUCCESS)

lst_filelogs = list(mapping_file.keys())

for in_file in lst_filelogs:
    etl_id = "etl_{0}".format(in_file.lower())

    in_path_template = mapping_file.get(in_file).format(root_path_data_ingame)
    etl_in_params = json.dumps({
        "in_path": {
            "render_type": "ts",
            "params": in_path_template
        }
    })

    step_check_exists = task_check_hdfs_exists(dag, product_id, in_file, in_path_template)

    # step_etl = BashOperator(
    #     bash_command=pyspark_submit_etl_job_template,
    #     params={'productID': product_id, 'inFile': in_file, 'inParams': etl_in_params},
    #     task_id=etl_id,
    #     dag=dag,
    #     priority_weight=20,
    #     sla=sla_time
    # )

    step_start >> step_check_exists_SUCCESS >> step_check_exists >> step_etl >> step_end


