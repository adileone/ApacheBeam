import datetime
import io
import time

from airflow import models
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.pubsub_operator import PubSubPullOperator
from airflow.operators.python_operator import PythonOperator

from google.cloud import storage
from zipfile import ZipFile
from zipfile import is_zipfile

from google.cloud import pubsub_v1

bucket_path = models.Variable.get("bucket_path")
project_id = models.Variable.get("project_id")
gce_zone = models.Variable.get("gce_zone")
gce_region = models.Variable.get("gce_region")
topic_id = models.Variable.get("topic_id")

default_args = {
    "start_date": days_ago(1),
    "dataflow_default_options": {
        "project": project_id,
        "region": gce_region,
        "zone": gce_zone,
        "temp_location": bucket_path + "/tmp/",
    },
}

with models.DAG(

    dag_id="job_dag",
    default_args=default_args,
    schedule_interval=datetime.timedelta(seconds=5),

) as dag:

    subscription = 'unzip-sub'

    t4 = PubSubPullOperator(task_id='pull-unzip-message', ack_messages=True, project=project_id, subscription=subscription, max_messages=1, ti)
    
    def has_next():
        encoded = kwargs['ti'].xcom_pull(task_ids='pull-unzip-message', key="return_value")
        if (encoded == []):
            return 'done'
        else:
            return 't5'

    branch_task = BranchPythonOperator(
        task_id='branching',
        python_callable=has_next,
        dag=dag
    )

    done = DummyOperator(task_id='complete')

    t5 = DataflowTemplateOperator(
        task_id="run_pipeline",
        job_name='mlb_composer_job',
        template="gs://beambinaries/templates/customTemplateBQ4",
        parameters={
            "header": "Name,Team,Position,Height,Weight,Age"
            "domain": "{{ task_instance.xcom_pull(task_ids='unzip_archive') }}",
        },
    )


t4 >> branch_task >> [done, t5]
t5 >> compute_header >> launch_pipeline


    
