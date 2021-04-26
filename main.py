# Standard libraries
from __future__ import absolute_import
import os
import subprocess

# Google Cloud APIs
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Spark APIs
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, LongType, DoubleType
from pyspark.sql.functions import when, upper

# Global Config
#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './data/key.json'
spark = SparkSession.builder.config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar').getOrCreate()
sc = spark.sparkContext
project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')

def verifyOrCreateDataset (client: bigquery.Client, dataset_name: str) -> None:
  # Set dataset_id to the ID of the dataset to determine existence.
  dataset_id = '.'.join([client.project, dataset_name])

  try:
    client.get_dataset(dataset_id)  # Make an API request.
    print(f'Dataset {dataset_id} already exists.')
  except NotFound:
    print(f'Dataset {dataset_id} is not found.')
    dataset = bigquery.Dataset(dataset_id)
    dataset = client.create_dataset(dataset)
    print(f'Created dataset `{dataset.dataset_id}`.')


def verifyOrCreateBigQueryTable (client: bigquery.Client, dataset_name: str, table_name: str, schema: list) -> bigquery.Table:
  # Set table_id to the ID of the table to determine existence.
  table_id = f'{client.project}.{dataset_name}.{table_name}'

  try:
    table = client.get_table(table_id)  # Make an API request.
    if table.schema != schema:
      print(f'Table {table_id} exists with incorrect schema. Deleting...')
      client.delete_table(table_id, not_found_ok=True)  # Make an API request
      return verifyOrCreateBigQueryTable(client, dataset_name, table_name, schema)
    print(f'Table {table_id} already exists with correct schema.')
  except NotFound:
    print(f'Table {table_id} is not found.')
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(f'Created table `{table_id}`.')
  return table


def loadDataTableFromGoogleStorage (dataset_name: str, table_name: str) -> None:
  # Construct a BigQuery client object.
  client = bigquery.Client()

  schema = [
    bigquery.SchemaField('ID', 'INTEGER'),
    bigquery.SchemaField('CASE_STATUS', 'STRING'),
    bigquery.SchemaField('EMPLOYER_NAME', 'STRING'),
    bigquery.SchemaField('SOC_NAME', 'STRING'),
    bigquery.SchemaField('JOB_TITLE', 'STRING'),
    bigquery.SchemaField('FULL_TIME_POSITION', 'STRING'),
    bigquery.SchemaField('PREVAILING_WAGE', 'STRING'),
    bigquery.SchemaField('YEAR', 'STRING'),
    bigquery.SchemaField('WORKSITE', 'STRING'),
    bigquery.SchemaField('lon', 'STRING'),
    bigquery.SchemaField('lat', 'STRING'),
  ]
  # Verify/Make the dataset and table
  verifyOrCreateDataset(client, dataset_name)
  table = verifyOrCreateBigQueryTable(client, dataset_name, table_name, schema)

  if table.num_rows <= 0:
    # Set table_id to the ID of the table to create.
    table_id = '.'.join([client.project, dataset_name, table_name])

    job_config = bigquery.LoadJobConfig(
      schema=schema,
      skip_leading_rows=1,
      # The source format defaults to CSV, so the line below is optional.
      source_format=bigquery.SourceFormat.CSV,
    )

    uri = f'gs://{bucket}/h1b_kaggle.csv'
    print(f'Loading `{table_id}` from `{uri}`')

    load_job = client.load_table_from_uri(
      uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print(f'Loaded {destination_table.num_rows} rows.')
  else:
    print(f'Table has {table.num_rows} rows.')


def loadBigQueryToHdfs (output_directory: str, dataset_name: str, table_name: str) -> None:
  # Create table name
  bq_table = '.'.join([project, dataset_name, table_name])

  # Check if files have already been copied
  result = subprocess.run(['hdfs', 'dfs', '-test', '-e', output_directory])

  # Copy to HDFS if files are not present
  if result.returncode != 0:
    print(f'Migrating data from `{bq_table}` to `{output_directory}` in HDFS.')
    # Make intermediate directory
    subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', 'project'])
    
    # Read in BigQuery table
    df = spark.read.format('bigquery').load(bq_table)

    # Clean fields
    df = df.withColumn('YEAR', when(df['YEAR'] == 'NA', None).otherwise(df['YEAR']))
    df = df.withColumn('YEAR', df['YEAR'].cast(IntegerType()))
    df = df.withColumn('SOC_NAME', upper(df['SOC_NAME']))
    df = df.withColumn('PREVAILING_WAGE', when(df['PREVAILING_WAGE'] == 'NA', None).otherwise(df['PREVAILING_WAGE']))
    df = df.withColumn('PREVAILING_WAGE', df['PREVAILING_WAGE'].cast(LongType()))
    df = df.withColumn('lat', when(df['lat'] == 'NA', None).otherwise(df['lat']))
    df = df.withColumn('lat', df['lat'].cast(DoubleType()))
    df = df.withColumn('lon', when(df['lon'] == 'NA', None).otherwise(df['lon']))
    df = df.withColumn('lon', df['lon'].cast(DoubleType()))

    # Save to local HDFS as parquet
    df.write.parquet(output_directory)
    print('Data saved as parquet format.')
  else:
    print('Data already migrated to HDFS.')


def loadFromHdfs (directory: str) -> pyspark.sql.DataFrame:
  df = spark.read.format('parquet').load(directory)
  print(f'{df.count()} records read in from HDFS.')
  return df


def queries (df: pyspark.sql.DataFrame) -> None:
  df.createOrReplaceTempView('h1b')
  query_3a = spark.sql('''
    SELECT
      COUNT(*) AS certified_florida_visas_2016
    FROM h1b 
    WHERE
      CASE_STATUS LIKE 'CERTIFIED%' AND
      WORKSITE LIKE '%FLORIDA' AND
      YEAR = 2016
  ''')
  query_3bi = spark.sql('''
    SELECT
      outt.SOC_NAME,
      COUNT(outt.SOC_NAME) / inn.occupation_count AS max_certification_rate
    FROM h1b outt
    INNER JOIN (
      SELECT
        SOC_NAME,
        COUNT(SOC_NAME) AS occupation_count
      FROM h1b
      GROUP BY
        SOC_NAME
      ORDER BY
        COUNT(SOC_NAME) DESC
      LIMIT 10
    ) AS inn ON
      outt.SOC_NAME = inn.SOC_NAME
    WHERE
      outt.CASE_STATUS LIKE 'CERTIFIED%'
    GROUP BY
      outt.SOC_NAME,
      inn.occupation_count
    ORDER BY
      COUNT(outt.SOC_NAME) / inn.occupation_count DESC
    LIMIT 1
  ''')
  query_3bii = spark.sql('''
    SELECT
      outt.SOC_NAME,
      COUNT(outt.SOC_NAME) / inn.occupation_count AS min_certification_rate
    FROM h1b outt
    INNER JOIN (
      SELECT
        SOC_NAME,
        COUNT(SOC_NAME) AS occupation_count
      FROM h1b
      GROUP BY
        SOC_NAME
      ORDER BY
        COUNT(SOC_NAME) DESC
      LIMIT 10
    ) AS inn ON
      outt.SOC_NAME = inn.SOC_NAME
    WHERE
      outt.CASE_STATUS LIKE 'CERTIFIED%'
    GROUP BY
      outt.SOC_NAME,
      inn.occupation_count
    ORDER BY
      COUNT(outt.SOC_NAME) / inn.occupation_count
    LIMIT 1
  ''')
  query_3c = spark.sql('''
    SELECT
      outt.YEAR,
      COUNT(outt.CASE_STATUS) AS certifications,
      inn.total_cases,
      COUNT(outt.CASE_STATUS) / inn.total_cases AS certification_rate
    FROM h1b outt
    INNER JOIN (
      SELECT
        YEAR,
        COUNT(CASE_STATUS) AS total_cases
      FROM h1b
      GROUP BY
        YEAR
    ) AS inn ON
      outt.YEAR = inn.YEAR
    WHERE
      outt.CASE_STATUS LIKE 'CERTIFIED%'
    GROUP BY
      outt.YEAR,
      inn.total_cases
    ORDER BY
      outt.YEAR
  ''')
  print('### QUERY 3a ####################################')
  query_3a.show()
  print('#################################################')
  print('### QUERY 3b ####################################')
  query_3bi.show(truncate=False)
  query_3bii.show(truncate=False)
  print('#################################################')
  print('### QUERY 3c ####################################')
  query_3c.show()
  print('#################################################')


def main () -> None:
  dataset_name = 'FinalProject'
  table_name = 'h1b'
  hdfs_directory = 'hdfs:///user/Nicolas/project/parquet'
  loadDataTableFromGoogleStorage(dataset_name, table_name)
  loadBigQueryToHdfs(hdfs_directory, dataset_name, table_name)
  df = loadFromHdfs(hdfs_directory)
  queries(df)


if __name__ == '__main__':
  main()