from datetime import timedelta
import datetime, os, time
import pandas as pd
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import copy
import pymongo, os, requests
from bson.objectid import ObjectId

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
P_API = "http://127.0.0.1:5000/api"
P_API_EXECUTE = os.path.join(P_API, "execute")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["airflow"]
dag_config_db = mydb["dag_config"]


def update_pause(dag_id):
    dag_config_db.find_one_and_update({'_id': ObjectId(dag_id)}, {"$set": {"status": "PAUSE"}}, upsert=False)


def update_running(dag_id):
    dag_config_db.find_one_and_update({'_id': ObjectId(dag_id)}, {"$set": {"status": "RUNNING"}}, upsert=False)


def update_wait_interval(dag_id):
    dag_config_db.find_one_and_update({'_id': ObjectId(dag_id)}, {"$set": {"status": "WAITING INTERVAL"}},
                                      upsert=False)


def update_fail(dag_id):
    dag_config_db.find_one_and_update({'_id': ObjectId(dag_id)}, {"$set": {"pre_interval_status": "fail"}},
                                      upsert=False)
    pass


def update_success(dag_id):
    dag_config_db.find_one_and_update({'_id': ObjectId(dag_id)}, {"$set": {"pre_interval_status": "success"}},
                                      upsert=False)
    pass


def update_last_time_interval(dag_id):
    dag_config_db.find_one_and_update({'_id': ObjectId(dag_id)},
                                      {"$set": {"last_time_interval": str(datetime.datetime.utcnow())}},
                                      upsert=False)
    pass


def get_default_transfer_args():
    default_args = {
        'owner': "airflow",
        'depends_on_past': False,
        'start_date': days_ago(0),
        'email': ['vmo@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(seconds=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'dag': dag,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    }
    return default_args


def run_transfer(dag_config, p_config):
    dag_id = dag_config["dag_id"]
    p_id = p_config["p_id"]
    response = requests.post(P_API_EXECUTE, json=p_config)
    if response.status_code != 200:
        update_fail(dag_id)
    return


def create(dag_config, p_config):
    dag_id = dag_config["dag_id"]
    p_id = p_config["p_id"]
    dag = DAG(
        dag_id=dag_id,
        default_args=get_default_transfer_args(),
        description=dag_config["description"],
        schedule_interval=dag_config["schedule_interval"]
    )

    set_running_task = PythonOperator(
        task_id='set_running_task',
        python_callable=update_running,
        op_kwargs={'dag_id': dag_id},
        dag=dag,
        retries=2,
        retry_delay=timedelta(seconds=5)
    )

    set_last_time_interval_task = PythonOperator(
        task_id='set_last_time_interval_task',
        python_callable=update_last_time_interval,
        op_kwargs={'dag_id': dag_id},
        dag=dag,
        retries=2,
        retry_delay=timedelta(seconds=5)
    )

    run_transfer_task = PythonOperator(
        task_id='run_transfer_task',
        python_callable=run_transfer,
        op_kwargs={'dag_config': dag_config, "p_config": p_config},
        dag=dag,
        retries=0
    )
    set_running_task >> set_last_time_interval_task >> run_transfer_task

    return dag


dag_config_objects = dag_config_db.find({})

for dag_config_object in dag_config_objects:
    dag_config = dag_config_object["dag_config"]
    p_config = dag_config_object["p_config"]
    globals()[dag_config["dag_id"]] = create(dag_config, p_config)
