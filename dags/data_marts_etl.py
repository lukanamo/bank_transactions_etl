from airflow.decorators import dag
from airflow.models import Variable
from airflow.utils.dates import days_ago

from dags.bank_transactions_etl import CUSTOMER_FILE_NAME
from tasks.submit_spark_job import submit_spark_job
from utils.logger import logger

DATA_MARTS_SCHEDULE_INTERVAL = Variable.get('DATA_MARTS_SCHEDULE_INTERVAL')

# dataproc args
DATAPROC_REGION = Variable.get('DATAPROC_REGION')
DATAPROC_CLUSTER_NAME = Variable.get('DATAPROC_CLUSTER_NAME')
PATH_TO_PYSPARK_DATA_MARTS_JOB = Variable.get('PATH_TO_PYSPARK_DATA_MARTS_JOB')

# pyspark job args
VALID_TABLE_NAME = Variable.get('VALID_TABLE_NAME')
DATASET_NAME = Variable.get('DATASET_NAME')
DATAPROC_TEMP_BUCKET = Variable.get('DATAPROC_TEMP_BUCKET')

# python_file_uris
CUSTOM_PACKAGE_URI = Variable.get('CUSTOM_PACKAGE_URI')

default_args = {
    'start_date': days_ago(1),
    'retries': 2
}


@dag(
    default_args=default_args,
    schedule_interval=DATA_MARTS_SCHEDULE_INTERVAL,
    catchup=False,
    description='Submit spark job to create data marts from the bank transaction data'
)
def bank_data_marts_etl():
    logger.info('Starting the bank_data_marts_etl DAG.')

    etl_task = submit_spark_job(
        dataproc_region=DATAPROC_REGION,
        dataproc_cluster_name=DATAPROC_CLUSTER_NAME,
        path_to_pyspark_job=PATH_TO_PYSPARK_DATA_MARTS_JOB,
        pyspark_job_args=list(
            VALID_TABLE_NAME, DATASET_NAME, DATAPROC_TEMP_BUCKET
        ),
        python_file_uris=list(CUSTOMER_FILE_NAME)
    )

    etl_task

    logger.info('ETL task for data marts scheduled.')


bank_data_marts_etl_dag = bank_data_marts_etl()
