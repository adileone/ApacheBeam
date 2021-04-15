import datetime
import io
import time

from airflow import models
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.pubsub_sensor import PubSubPullSensor
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

    dag_id="unzip_dag",
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),

) as dag:

    subscription = 'unzip-sub'

    t4 = PubSubPullSensor(task_id='pull-unzip-message', ack_messages=True, project=project_id, subscription=subscription, max_messages=1)

    def get_message(**kwargs):
        encoded = kwargs['ti'].xcom_pull(task_ids='pull-unzip-message', key="return_value")
        return encoded[0]["message"]["attributes"]["objectId"]

    t5 = PythonOperator(
        task_id='get_message',
        python_callable=get_message,
        provide_context=True,
        dag=dag,
    )

    def zipextract(bucketname, **kwargs):

        storage_client = storage.Client(project=project_id)
        bucket = storage_client.get_bucket(bucketname)

        encoded = kwargs['ti'].xcom_pull(task_ids='get_message')

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

    def pushToPubSub(**kwargs):

        domains = kwargs['ti'].xcom_pull(task_ids='unzip_archive').split(",")
        
        publisher = pubsub_v1.PublisherClient()
        topic_path = 'projects/'+ project_id +'/topics/'+topic_id

        for domain in domains:
            data = domain
            data = data.encode("utf-8")
            publisher.publish(topic_path, data)

        return "Published messages to " + topic_path


    t7 = PythonOperator(
        task_id='push_pubSub_archive_domains',
        python_callable=pushToPubSub,
        provide_context=True,
        dag=dag,
    )

    (t4 >> t5 >> t6 >> t7)
