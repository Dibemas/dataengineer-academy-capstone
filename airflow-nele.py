from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator
import datetime as dt

dag = DAG(
    dag_id="nele-capstone-scheduler",
    description="Scheduler for the capstone project by Nele & Johan",
    default_args={"owner": "Airflow"},
    schedule_interval="@once",
    start_date=dt.datetime(2023, 1, 1),
)
batch_job_name = "nele-" + dt.datetime.now().strftime("%Y%m%d%H%M%s")

submit_batch_job = BatchOperator(
    task_id="submit_batch_job",
    job_name=batch_job_name,
    job_queue="academy-capstone-pxl-2023-job-queue",
    job_definition="nele-capstone-job:2",
    dag=dag,
    overrides= "")