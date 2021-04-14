from airflow import DAG, settings
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

def launch_jobs_subdag(parent_dag_name, child_dag_name, start_date, schedule_interval, parent_dag=None):
    
    dag = DAG(
            parent_dag_name + "." + child_dag_name,
            schedule_interval=schedule_interval,
            start_date=start_date,
        )

    def domainLaunchtemplate(domain, **kwargs):
        return domain    

    if len(parent_dag.get_active_runs()) > 0:
        domainList = parent_dag.get_task_instances(settings.Session, start_date=parent_dag.get_active_runs()[-1])[-1].xcom_pull(
            dag_id='%s.%s' % (parent_dag_name, 'extract_domains'),
            task_ids='list')
        if domainList:
            for domain in domainList: 
                PythonOperator( task_id='domainLaunchtemplate',
                                    python_callable=domainLaunchtemplate,
                                    op_kwargs={'domain': domain},
                                    provide_context=True,
                                    dag=dag) 

    return dag