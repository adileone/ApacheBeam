import io

# from datetime import datetime
import datetime

from composer.subdags.extract_domain_subdag import extract_domain_subdag
from composer.subdags.launch_jobs_subdag import launch_jobs_subdag
from airflow.api.common.experimental.get_task_instance import get_task_instance

from airflow.contrib.sensors.pubsub_sensor import PubSubPullSensor

from airflow import DAG
from airflow import models
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.dates import days_ago

from airflow.operators.python_operator import PythonOperator

from google.cloud import storage
from zipfile import ZipFile
from zipfile import is_zipfile

DAG_NAME = 'VF-DriveTest-Workflow'

bucket_path = models.Variable.get("bucket_path")
project_id = models.Variable.get("project_id")
gce_zone = models.Variable.get("gce_zone")
gce_region = models.Variable.get("gce_region")
startDate = days_ago(1)

# default_args = {
#     # Tell airflow to start one day ago, so that it runs as soon as you upload it
#     "start_date": startDate,
#     "dataflow_default_options": {
#         "project": project_id,
#         # Set to your region
#         "region": gce_region,
#         # Set to your zone
#         "zone": gce_zone,
#         # This is a subfolder for storing temporary files, like the staged pipeline job.
#         "temp_location": bucket_path + "/tmp/",
#     },
# }

# with models.DAG(

#     # The id you will see in the DAG airflow page
#     DAG_NAME,
#     default_args=default_args,
#     # The interval with which to schedule the DAG
#     schedule_interval=datetime.timedelta(days=1),  # Override to match your needs

# ) as dag:

dag = DAG(DAG_NAME,
          description='Test workflow',
          catchup=False,
          schedule_interval='0 0 * * *',
          start_date=startDate)

# extract_domain = SubDagOperator(
#     subdag=extract_domain_subdag(DAG_NAME,
#                                 "extract_domains",
#                                 startDate,
#                                 dag.schedule_interval),
#     task_id='extract_domains',
#     dag=dag
# )

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

def compute_header(domain):
    return ""

def launch_jobs(bucketname, **kwargs):

    dataflow_default_options ={
               'project': 'my-gcp-project',
               'region': 'europe-west1',
               'zone': 'europe-west1-d',
               'tempLocation': 'gs://my-staging-bucket/staging/'
               }

    domains = kwargs['ti'].xcom_pull(task_ids='unzip_archive').split(",")

    storage_client = storage.Client(project=project_id)
    input_bucket = storage_client.get_bucket(bucketname)

    dataflow_client = dataflow.Client(project=project_id)

    for domain in domains:
        header = compute_header(domain)
        dataflow_client.start_template_job


    input_bucket.create()


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


t7 = PythonOperator(
    task_id='launch_jobs',
    python_callable=launch_jobs,
    op_kwargs={'bucketname': bucket_path},
    provide_context=True,
    dag=dag
)

# launch_jobs = SubDagOperator(
#     subdag=launch_jobs_subdag(DAG_NAME,
#                             "launch_jobs",
#                             startDate,
#                             dag.schedule_interval,
#                             parent_dag=dag),
#     task_id='launch_jobs',
#     dag=dag
# )

def domainLaunchtemplate(domain, **kwargs):
        print
        return domain    

    # if len(parent_dag.get_active_runs()) > 0:
    
    # startDate=parent_dag.get_active_runs()
    # if (len(startDate) == 0):
    #     raise Exception("parent dag: "+parent_dag_name+" active runs: "+len(startDate))
        
    
    # taskInstance = parent_dag.get_task_instances(settings.Session, start_date=startDate[-1])

    # if (len(taskInstance) == 0):
    #     raise Exception("no taskInstancesfound")

    # # taskInstance = parent_dag.get_task_instances(settings.Session, start_date=parent_dag.get_active_runs()[-1])
    # domainList = taskInstance[-1].xcom_pull(dag_id=parent_dag_name+'.'+'extract_domains', task_ids='unzip_archive')

    # ti = get_task_instance(parent_dag_name+'.'+'extract_domains', 'unzip_archive', start_date)
   
    # ti = get_task_instance(parent_dag_name, 'extract_domains', start_date)
    # domainList = ti.xcom_pull(dag_id=parent_dag_name+'.'+'extract_domains', task_ids='unzip_archive')

        # if domainList:

# domainsList = "YT,VT,BV"

# launch_jobs = SubDagOperator(
#     subdag=launch_jobs_subdag(parent_dag_name=DAG_NAME,
#                             child_dag_name="launch_jobs",
#                             domain_list= domainsList,
#                             start_date=startDate,
#                             schedule_interval=dag.schedule_interval,
#                             parent_dag=dag
#                            ),
#     task_id='launch_jobs',
#     dag=dag
# )
# ti = get_task_instance(DAG_NAME, 'unzip_archive', startDate)
# domainList = ti.xcom_pull(task_ids='pull-messages', key="return_value")

# domains = domainList.split(",")
# for domain in domains: 
#     PythonOperator( task_id='domainLaunchtemplate',
#                         python_callable=domainLaunchtemplate,
#                         op_kwargs={'domain': domain},
#                         provide_context=True,
#                         dag=dag) 


(t4 >> t5 >> t6 >> launch_jobs)