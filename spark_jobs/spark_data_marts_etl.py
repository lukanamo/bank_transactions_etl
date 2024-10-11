from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as _sum, avg
from build.lib.utils.bigquery_utils import create_dataset_if_not_exists
from utils.bigquery_utils import create_table_if_not_exists
from utils.logger import logger
import sys

logger.info('Starting pyspark job for data marts creation')

spark = SparkSession.builder.appName('Bank Data Marts ETL').getOrCreate()

VALID_TABLE_NAME = sys.argv[1]
DATASET_NAME = sys.argv[2]
DATAPROC_TEMP_BUCKET = sys.argv[3]

valid_data_df = spark.read \
    .format('bigquery') \
    .option('table', f'{DATASET_NAME}.{VALID_TABLE_NAME}') \
    .load()

logger.info(f'Successfully read valid data from {VALID_TABLE_NAME}')

transactions_per_customer_df = valid_data_df.groupby('customer_id') \
    .agg(
    count('transaction_id').alias('num_transactions'),
    _sum('amount').alias('total_amount'),
    avg('amount').alias('average_transaction_amount')
)

logger.info('Data marts created successfully')

datamarts_dataset_name = 'datamarts_dataset'
transactions_per_customer_table_name = 'transactions_per_customer'
create_dataset_if_not_exists(datamarts_dataset_name)
create_table_if_not_exists(transactions_per_customer_table_name)

transactions_per_customer_df.write \
    .format('bigquery') \
    .option('table',
            f'{datamarts_dataset_name}.{transactions_per_customer_table_name}') \
    .option('temporaryGcsBucket', DATAPROC_TEMP_BUCKET) \
    .mode('append') \
    .save()

logger.info('Data marts successfully loaded in bq')
