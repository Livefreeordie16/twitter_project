from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from datetime import timedelta

the_command = "python3 /opt/airflow/dags/scripts/twitter_local.py"

with DAG("snowpipe_prj", start_date=datetime(2025, 1, 17),
     schedule_interval=None, catchup=False) as dag:
        retrieve_twitter = BashOperator(
                task_id="retrieve_twitter",
                bash_command=the_command        
        )


        
     