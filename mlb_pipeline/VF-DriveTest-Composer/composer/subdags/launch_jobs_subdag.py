from airflow import DAG, settings
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.api.common.experimental.get_task_instance import get_task_instance


def launch_jobs_subdag(parent_dag_name, child_dag_name, domain_list, start_date, schedule_interval, parent_dag=None):
    
    dag = DAG(
            parent_dag_name + "." + child_dag_name,
            schedule_interval=schedule_interval,
            start_date=start_date,
            # start_date=parent_dag.latest_execution_date
        )

    def domainLaunchtemplate(domain, **kwargs):
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

    # # ti = get_task_instance(parent_dag_name+'.'+'extract_domains', 'unzip_archive', start_date)
   
    # # ti = get_task_instance(parent_dag_name, 'extract_domains', start_date)
    # # domainList = ti.xcom_pull(dag_id=parent_dag_name+'.'+'extract_domains', task_ids='unzip_archive')

    # ti = get_task_instance(parent_dag_name, 'unzip_archive', start_date)
    # domainList = ti.xcom_pull(dag_id=parent_dag_name, task_ids='unzip_archive')
        # if domainList:
    domains = domain_list.split(",")
    for domain in domains: 
        PythonOperator( task_id='domainLaunchtemplate',
                            python_callable=domainLaunchtemplate,
                            op_kwargs={'domain': domain},
                            provide_context=True,
                            dag=dag) 

    return dag