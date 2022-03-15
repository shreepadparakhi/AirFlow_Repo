#importing libraries:
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.email import send_email
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta,datetime

def Success_Email(context):
    dr = context.get("dag_run")
    msg = "DAG has been completed"
    subject = f"DAG {dr} Completed successfully"
    send_email(to=['shreeparakhi96@gmail.com'],subject=subject,html_content=msg)

def Failure_Email(context):
    dr = context.get("dag_run")
    msg = "DAG has been failied"
    subject = f"DAG {dr} failed"
    send_email(to=['shreeparakhi96@gmail.com'],subject=subject,html_content=msg)

# These args will get passed on to the python operator
default_args = {
    'owner': 'shreepad',
    'depends_on_past': True,
    'start_date': datetime(2022, 3, 14),
    'email': ['shreeparakhi96@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback' : Failure_Email
}

# define the DAG
dag = DAG(
    'emp_airflow_dag',
    default_args=default_args,
    description='emp table airflow dag',
    schedule_interval=timedelta(days=1),
    catchup=False
)

#spark submit task
emp_spark_submit_job = SparkSubmitOperator(application='/home/saif/PycharmProjects/cohort_c9_training/emp_airflow.py',
    task_id='emp_spark_submit_job',
    conf={'master':'yarn','deploy-mode':'client'},
    driver_memory='1g',
    executor_memory='2g',
    num_executors=1,
    executor_cores=4,
    on_success_callback = Success_Email,
    dag=dag
)

#dummy
start_task = DummyOperator(
    task_id = 'start_Task',
    dag=dag
)

end_task = DummyOperator(
    task_id = 'end_task',
    dag=dag
)

start_task >> emp_spark_submit_job >> end_task