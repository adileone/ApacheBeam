import datetime

from composer.subdags.extract_domain_subdag import extract_domain_subdag
from composer.subdags.launch_jobs_subdag import launch_jobs_subdag

from airflow import DAG
from airflow import models
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.dates import days_ago

DAG_NAME = 'VF-DriveTest-Workflow'

bucket_path = models.Variable.get("bucket_path")
project_id = models.Variable.get("project_id")
gce_zone = models.Variable.get("gce_zone")
gce_region = models.Variable.get("gce_region")

# default_args = {
#     # Tell airflow to start one day ago, so that it runs as soon as you upload it
#     "start_date": days_ago(1),
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
          start_date=days_ago(1))

extract_domain = SubDagOperator(
    subdag=extract_domain_subdag(DAG_NAME,
                                "extract_domains",
                                days_ago(1),
                                dag.schedule_interval),
    task_id='extract_domains',
    dag=dag
)

launch_jobs = SubDagOperator(
    subdag=launch_jobs_subdag(DAG_NAME,
                            "launch_jobs",
                            days_ago(1),
                            dag.schedule_interval,
                            parent_dag=dag),
    task_id='launch_jobs',
    dag=dag
)

extract_domain >> launch_jobs