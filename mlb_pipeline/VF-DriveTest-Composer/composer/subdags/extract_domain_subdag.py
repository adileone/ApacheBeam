import io

from airflow import models
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.sensors.pubsub_sensor import PubSubPullSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import PythonOperator

from google.cloud import storage
from zipfile import ZipFile
from zipfile import is_zipfile

def extract_domain_subdag(parent_dag_name, child_dag_name, start_date, schedule_interval):

    dag = DAG(
            parent_dag_name + "." + child_dag_name,
            schedule_interval=schedule_interval,
            start_date=start_date,
        )

    bucket_path = models.Variable.get("bucket_path")
    project_id = models.Variable.get("project_id")

    subscription = 'test-sub'  # Cloud Pub/Sub subscription

    t4 = PubSubPullSensor(task_id='pull-messages', ack_messages=True, project=project_id, subscription=subscription, max_messages=1)

    def print_context(**kwargs):

        encoded = kwargs['ti'].xcom_pull(task_ids='pull-messages', key="return_value")
        
        return encoded[0]["message"]["attributes"]["objectId"]

    t5 = PythonOperator(
        task_id='decode_message',
        python_callable=print_context,
        provide_context=True,
        dag=dag,
    )

    def zipextract(bucketname, **kwargs):

        storage_client = storage.Client(project=project_id )
        bucket = storage_client.get_bucket(bucketname)

        encoded = kwargs['ti'].xcom_pull(task_ids='decode_message')

        zipfilename_with_path = encoded
        destination_blob_pathname = zipfilename_with_path

        blob = bucket.blob(destination_blob_pathname)
        zipbytes = io.BytesIO(blob.download_as_string())

        contentFileNameList = []

        if is_zipfile(zipbytes):
            with ZipFile(zipbytes, 'r') as myzip:
                for contentfilename in myzip.namelist():
                    contentfile = myzip.read(contentfilename)
                    blob = bucket.blob(zipfilename_with_path + "/" + contentfilename)
                    contentFileNameList.append(contentfilename)
                    blob.upload_from_string(contentfile)

        contentFileNameList = [element.split("-")[4].split("_")[0] for element in contentFileNameList]           

        return ",".join(set(contentFileNameList))           

    t6 = PythonOperator(
        task_id='unzip_archive',
        python_callable=zipextract,
        op_kwargs={'bucketname': bucket_path},
        provide_context=True,
        dag=dag,
    )

    (t4 >> t5 >> t6)

    return dag