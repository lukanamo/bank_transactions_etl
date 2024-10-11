from datetime import datetime
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from utils.logger import logger

DATAPROC_REGION = Variable.get('DATAPROC_REGION')
PATH_TO_PYSPARK_JOB = Variable.get('PATH_TO_PYSPARK_JOB')

def generate_batch_id():
    return f"batch-{datetime.now().strftime('%Y%m%d%H%M%S')}"

def submit_spark_job():
    try:
        logger.info("Submitting Spark job.")

        batch_id = generate_batch_id()
        logger.info(f"Generated batch ID: {batch_id}")

        spark_job_operator = DataprocCreateBatchOperator(
            task_id='submit_spark_job_serverless',
            batch_id=batch_id,
            region=DATAPROC_REGION,
            batch={
                'pyspark_batch': {'main_python_file_uri': PATH_TO_PYSPARK_JOB}
                # 'environment_config': {
                #     'execution_config': {
                #         'max_workers': 2,
                #         'worker_machine_type': 'n1-standard-2'
                #     }
                # }
            }
        )

        logger.info("Dataproc batch job created successfully.")
        return spark_job_operator
    except Exception as e:
        logger.error(f"Error submitting Spark job: {e}")
        raise
