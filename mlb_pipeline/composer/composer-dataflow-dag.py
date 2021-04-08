import datetime

from airflow import models
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.pubsub_sensor import PubSubPullSensor
from airflow.operators.python_operator import PythonOperator

from google.cloud import storage
from zipfile import ZipFile
from zipfile import is_zipfile
import io

bucket_path = models.Variable.get("bucket_path")
project_id = models.Variable.get("project_id")
gce_zone = models.Variable.get("gce_zone")
gce_region = models.Variable.get("gce_region")

# echo_template = '''
# {% for m in task_instance.xcom_pull(task_ids='pull-messages') %}
#     echo "AckID: {{ m.get('ackId') }}, Base64-Encoded: {{ m.get('message') }}"
# {% endfor %}
# '''

default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    "start_date": days_ago(1),
    "dataflow_default_options": {
        "project": project_id,
        # Set to your region
        "region": gce_region,
        # Set to your zone
        "zone": gce_zone,
        # This is a subfolder for storing temporary files, like the staged pipeline job.
        "temp_location": bucket_path + "/tmp/",
    },
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
    # The id you will see in the DAG airflow page
    "new_dag_20sec",
    default_args=default_args,
    # The interval with which to schedule the DAG
    schedule_interval=datetime.timedelta(days=1),  # Override to match your needs
) as dag:

    subscription = 'test-sub'  # Cloud Pub/Sub subscription

    t4 = PubSubPullSensor(task_id='pull-messages', ack_messages=True, project=project_id, subscription=subscription, max_messages=1)

    def print_context(**kwargs):

        encoded = kwargs['ti'].xcom_pull(task_ids='pull-messages', key="return_value")
        
        # l=[]
        
        # for m in encoded :
        #     value = m["message"]["attributes"]["objectId"]
        #     l.append(value)

        # result = ",".join(map(str, l))

        # i=0
        # for m in encoded :
        #     value = m["message"]["attributes"]["objectId"]
        #     kwargs['ti'].xcom_push(key="message_"+str(i), value=value)
        #     i=i+1
        
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

        # zipfilename_with_path = encoded.split(',')[0]
        zipfilename_with_path = encoded
        destination_blob_pathname = zipfilename_with_path

        blob = bucket.blob(destination_blob_pathname)
        zipbytes = io.BytesIO(blob.download_as_string())

        if is_zipfile(zipbytes):
            with ZipFile(zipbytes, 'r') as myzip:
                for contentfilename in myzip.namelist():
                    contentfile = myzip.read(contentfilename)
                    blob = bucket.blob(zipfilename_with_path + "/" + contentfilename)
                    blob.upload_from_string(contentfile)

        return "gs://"+bucket_path+"/"+zipfilename_with_path + "/" + contentfilename             

    t6 = PythonOperator(
        task_id='unzip_archive',
        python_callable=zipextract,
        op_kwargs={'bucketname': bucket_path},
        provide_context=True,
        dag=dag,
    )                

    # t5 = BashOperator(task_id='echo-pulled-messages',
    #                   bash_command=echo_template)

    start_template_job = DataflowTemplateOperator(
        # The task id of your job
        task_id="dataflow_operator_run_pipeline",
        job_name='mlb_composer_job',
        # The name of the template that you're using.
        # Below is a list of all the templates you can use.
        # For versions in non-production environments, use the subfolder 'latest'
        # https://cloud.google.com/dataflow/docs/guides/templates/provided-batch#gcstexttobigquery
        template="gs://beambinaries/templates/customTemplate1",
        # Use the link above to specify the correct parameters for your template.
        parameters={
            "input": "{{ task_instance.xcom_pull(task_ids='unzip_archive') }}",
        },
    )

    (t4 >> t5 >> t6 >> start_template_job)