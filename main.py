# Standard libraries
from __future__ import absolute_import
import os
import argparse
import subprocess
from time import time
from pprint import pprint

# Google Cloud APIs
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Spark APIs
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import IntegerType, LongType, DoubleType
from pyspark.sql.functions import split, upper, trim, when


# Parse command line args
parser = argparse.ArgumentParser(description='H1B Visa Petition Analysis')
parser.add_argument('-f', '--force', action='store_true', help='always perform data transfers')
parser.add_argument('-q', '--quiet', action='store_true', help='do not print notifications')
parser.add_argument('--hdfs', default='/user/Nicolas/project/parquet', help='specify a HDFS directory to store data')
parser.add_argument('--dataset', default='FinalProject', help='specify a Google Cloud dataset name')
parser.add_argument('-s', '--source', default='/h1b_kaggle.csv', help='specify the path to a source data CSV file in Google Cloud Storage')
parser.add_argument('--table', default='h1b', help='specify a Google Cloud dataset table name')
parser.add_argument('--no-basic', action='store_true', help='do not execute basic queries')
parser.add_argument('--no-additional', action='store_true', help='do not execute additional queries')
parser.add_argument('--no-task', action='store_true', help='do not execute task queries')
parser.add_argument('--no-timing', action='store_true', help='do not execute timing queries')
args = parser.parse_args()

# Global Config
spark = SparkSession.builder.config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar').getOrCreate()
sc = spark.sparkContext
project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
gc_bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
hdfs_address = sc._jsc.hadoopConfiguration().get('dfs.namenode.rpc-address')


# Print only if quiet flag not specified
def arg_print (s: str) -> None:
  if args is None or not args.quiet:
    print(s)


# Verifies existence of or creates Google Cloud BigQuery Dataset
def verifyOrCreateDataset (client: bigquery.Client, dataset_name: str) -> None:
  # Set dataset_id to the ID of the dataset to determine existence.
  dataset_id = '.'.join([client.project, dataset_name])

  if args is not None and args.force:
    arg_print(f'Force deleting `{dataset_id}`...')
    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)

  # Try to get it, else create it
  try:
    client.get_dataset(dataset_id)  # Make an API request.
    arg_print(f'Dataset {dataset_id} already exists.')
  except NotFound:
    arg_print(f'Dataset {dataset_id} is not found.')
    dataset = bigquery.Dataset(dataset_id)
    dataset = client.create_dataset(dataset)
    arg_print(f'Created dataset `{dataset.dataset_id}`.')


# Verifies existence of or creates Google Cloud BigQuery Dataset Table
def verifyOrCreateBigQueryTable (client: bigquery.Client, dataset_name: str, table_name: str, schema: list) -> bigquery.Table:
  # Set table_id to the ID of the table to determine existence.
  table_id = f'{client.project}.{dataset_name}.{table_name}'

  # Try to get it, else create it
  try:
    table = client.get_table(table_id)  # Make an API request.
    # Verify that the schema matches, otherwise remake it
    if table.schema != schema:
      arg_print(f'Table {table_id} exists with incorrect schema. Deleting...')
      client.delete_table(table_id, not_found_ok=True)  # Make an API request
      return verifyOrCreateBigQueryTable(client, dataset_name, table_name, schema)
    arg_print(f'Table `{table_id}` already exists with correct schema.')
  except NotFound:
    arg_print(f'Table `{table_id}` is not found.')
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    arg_print(f'Created table `{table_id}`.')
  return table


# Load Google Cloud BigQuery Table from csv dataset in Google Cloud Storage
def loadDataTableFromGoogleStorage (dataset_name: str, table_name: str) -> None:
  # Construct a BigQuery client object.
  client = bigquery.Client()

  # Specify the schema for the BigQuery table
  # Data is not clean as columns with numerics have 'NA' as well
  # Data will be cleaned in import to HDFS
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

  # Populate table if there are no records
  if table.num_rows <= 0:
    # Set table_id to the ID of the table to create.
    table_id = '.'.join([client.project, dataset_name, table_name])

    # Job properties
    job_config = bigquery.LoadJobConfig(
      schema=schema,
      skip_leading_rows=1,
      # The source format defaults to CSV, so the line below is optional.
      source_format=bigquery.SourceFormat.CSV,
    )

    default_file = '/h1b_kaggle.csv'

    uri = f'gs://{gc_bucket}{args.source if args is not None else default_file}'
    arg_print(f'Loading `{table_id}` from `{uri}`.')

    # Load table from file asynchronously
    load_job = client.load_table_from_uri(
      uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    # Verify the load
    destination_table = client.get_table(table_id)  # Make an API request.
    arg_print(f'Loaded {destination_table.num_rows} rows.')
  else:
    arg_print(f'Table has {table.num_rows} rows.')


# Load local cluster HDFS with data from Google Cloud BigQuery
def loadBigQueryToHdfs (output_directory: str, dataset_name: str, table_name: str) -> None:
  # Create table name
  bq_table = '.'.join([project, dataset_name, table_name])

  # Clear local HDFS store for force
  if args is not None and args.force:
    subprocess.run(['hdfs', 'dfs', '-rm', '-r', output_directory])
  
  # Check if files have already been copied (shallow)
  result = subprocess.run(['hdfs', 'dfs', '-test', '-e', output_directory])

  # Copy to HDFS if files are not present
  if result.returncode != 0:
    arg_print(f'Migrating data from `{bq_table}` to `{output_directory}`.')
    # Make the directories along the way
    dirname = os.path.dirname(output_directory)
    arg_print(f'Making `{dirname}` directory.')
    subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', dirname])
    
    # Read in BigQuery table
    df = spark.read.format('bigquery').load(bq_table)

    # Clean fields ('NA' values replaced with None)
    df = df.withColumn('CASE_STATUS', when(df['CASE_STATUS'] == 'NA', None).otherwise(df['CASE_STATUS']))
    df = df.withColumn('EMPLOYER_NAME', when(df['EMPLOYER_NAME'] == 'NA', None).otherwise(df['EMPLOYER_NAME']))
    # Convert SOC_NAME to upper to group job categories
    df = df.withColumn('SOC_NAME', upper(df['SOC_NAME']))
    df = df.withColumn('SOC_NAME', when(df['SOC_NAME'] == 'NA', None).otherwise(df['SOC_NAME']))
    df = df.withColumn('JOB_TITLE', when(df['JOB_TITLE'] == 'NA', None).otherwise(df['JOB_TITLE']))
    df = df.withColumn('FULL_TIME_POSITION', when(df['FULL_TIME_POSITION'] == 'NA', None).otherwise(df['FULL_TIME_POSITION']))
    df = df.withColumn('PREVAILING_WAGE', when(df['PREVAILING_WAGE'] == 'NA', None).otherwise(df['PREVAILING_WAGE']))
    # Cast wages to long, year to integer, and latitude/longitude to double
    df = df.withColumn('PREVAILING_WAGE', df['PREVAILING_WAGE'].cast(LongType()))
    df = df.withColumn('YEAR', when(df['YEAR'] == 'NA', None).otherwise(df['YEAR']))
    df = df.withColumn('YEAR', df['YEAR'].cast(IntegerType()))
    df = df.withColumn('lat', when(df['lat'] == 'NA', None).otherwise(df['lat']))
    df = df.withColumn('lat', df['lat'].cast(DoubleType()))
    df = df.withColumn('lon', when(df['lon'] == 'NA', None).otherwise(df['lon']))
    df = df.withColumn('lon', df['lon'].cast(DoubleType()))
    
    # Additional columns (added for easier access to data)
    split_col = split(df['WORKSITE'], ',')
    df = df.withColumn('city', trim(split_col.getItem(0)))
    df = df.withColumn('state', trim(split_col.getItem(1)))

    # Save to local HDFS as parquet
    df.write.parquet(output_directory)
    arg_print('Data saved as parquet format.')
  else:
    arg_print('Data already migrated to HDFS.')


# Read local cluster HDFS parquet data into a PySpark DataFrame
def loadFromHdfs (directory: str) -> pyspark.sql.DataFrame:
  df = spark.read.format('parquet').load(directory)
  arg_print(f'{df.count()} records read in from HDFS.')
  return df


# Answering questions from part 3
def basicQueries (df: pyspark.sql.DataFrame) -> None:
  # Allow the DataFrame to be used in Spark SQL queries
  df.createOrReplaceTempView('h1b')
  # How many people in Florida had their visa applications certified in 2016?
  query_3a = spark.sql('''
    SELECT
      COUNT(*) AS certified_florida_visas_2016
    FROM h1b 
    WHERE
      CASE_STATUS LIKE 'CERTIFIED%' AND
      state = 'FLORIDA' AND
      YEAR = 2016
  ''')
  # Of the top 10 applied jobs, which one had the maximum certification rate?
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
  # Of the top 10 applied jobs, which one had the minimum certification rate?
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
  # What was the certification rate for jobs per year?
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
  # Display the query results
  print('\n### BASIC QUERIES ###############################\n')
  print('### QUERY 3a ####################################')
  query_3a.show()
  print('### QUERY 3b ####################################')
  query_3bi.show(truncate=False)
  query_3bii.show(truncate=False)
  print('### QUERY 3c ####################################')
  query_3c.show()


# Find additional insights as per part 4
def additionalQueries (df: pyspark.sql.DataFrame) -> None:
  # Allow the DataFrame to be used in Spark SQL queries
  df.createOrReplaceTempView('h1b')
  # Show all the records in the table
  query1 = spark.sql('''
    SELECT
      *
    FROM h1b
  ''')
  # Which certified jobs had the highest salaries?
  query2 = spark.sql('''
    SELECT
      JOB_TITLE,
      MAX(PREVAILING_WAGE) AS highest_salary
    FROM h1b
    WHERE
      CASE_STATUS LIKE 'CERTIFIED%'
    GROUP BY
      JOB_TITLE
    ORDER BY
      MAX(PREVAILING_WAGE) DESC
  ''')
  # Which certified jobs had the lowest salaries?
  query3 = spark.sql('''
    SELECT
      JOB_TITLE,
      MIN(PREVAILING_WAGE) AS lowest_salary
    FROM h1b 
    WHERE
      CASE_STATUS LIKE 'CERTIFIED%' AND
      PREVAILING_WAGE IS NOT NULL
    GROUP BY
      JOB_TITLE
    ORDER BY
      MIN(PREVAILING_WAGE)
  ''')
  # Which employers had the most certified H1B visa applications
  # and how much is the average worker there making?
  query4 = spark.sql('''
    SELECT
      outt.EMPLOYER_NAME,
      inn.application_count,
      SUM(outt.PREVAILING_WAGE) / COUNT(outt.PREVAILING_WAGE) AS average_pay
    FROM h1b outt
    INNER JOIN (
      SELECT
        EMPLOYER_NAME,
        COUNT(EMPLOYER_NAME) AS application_count
      FROM h1b
      WHERE
        CASE_STATUS LIKE 'CERTIFIED%'
      GROUP BY
        EMPLOYER_NAME
    ) AS inn ON
      outt.EMPLOYER_NAME = inn.EMPLOYER_NAME
    GROUP BY
      outt.EMPLOYER_NAME,
      inn.application_count
    ORDER BY
      inn.application_count DESC
  ''')
  # What were the highest growing roles for certified 
  # visa applications between 2011 and 2012?
  query5 = spark.sql('''
    SELECT
      outt.SOC_NAME,
      res_2011.2011_certifications,
      COUNT(outt.SOC_NAME) AS 2012_certifications,
      COUNT(outt.SOC_NAME) - res_2011.2011_certifications AS yoy_change
    FROM h1b outt
      INNER JOIN (
      SELECT
        outt.SOC_NAME,
        COUNT(outt.SOC_NAME) AS 2011_certifications
      FROM h1b outt
      INNER JOIN (
        SELECT
          *
        FROM h1b
        WHERE
          CASE_STATUS LIKE 'CERTIFIED%'
      ) AS certified ON
        certified.ID = outt.ID
      WHERE
        outt.YEAR = 2011
      GROUP BY
        outt.SOC_NAME
    ) AS res_2011 ON
      res_2011.SOC_NAME = outt.SOC_NAME
    WHERE
      outt.YEAR = 2012 AND
      outt.CASE_STATUS LIKE 'CERTIFIED%'
    GROUP BY
      outt.SOC_NAME,
      res_2011.2011_certifications
    ORDER BY
      (COUNT(outt.SOC_NAME) - res_2011.2011_certifications) DESC
  ''')
  # What were the highest growing roles for certified 
  # visa applications between 2012 and 2013?
  query6 = spark.sql('''
    SELECT
      outt.SOC_NAME,
      prev_yr.2012_certifications,
      COUNT(outt.SOC_NAME) AS 2013_certifications,
      COUNT(outt.SOC_NAME) - prev_yr.2012_certifications AS yoy_change
    FROM h1b outt
      INNER JOIN (
      SELECT
        outt.SOC_NAME,
        COUNT(outt.SOC_NAME) AS 2012_certifications
      FROM h1b outt
      INNER JOIN (
        SELECT
          *
        FROM h1b
        WHERE
          CASE_STATUS LIKE 'CERTIFIED%'
      ) AS certified ON
        certified.ID = outt.ID
      WHERE
        outt.YEAR = 2012
      GROUP BY
        outt.SOC_NAME
    ) AS prev_yr ON
      prev_yr.SOC_NAME = outt.SOC_NAME
    WHERE
      outt.YEAR = 2013 AND
      outt.CASE_STATUS LIKE 'CERTIFIED%'
    GROUP BY
      outt.SOC_NAME,
      prev_yr.2012_certifications
    ORDER BY
      (COUNT(outt.SOC_NAME) - prev_yr.2012_certifications) DESC
  ''')
  # What were the highest growing roles for certified 
  # visa applications between 2013 and 2014?
  query7 = spark.sql('''
    SELECT
      outt.SOC_NAME,
      prev_yr.2013_certifications,
      COUNT(outt.SOC_NAME) AS 2014_certifications,
      COUNT(outt.SOC_NAME) - prev_yr.2013_certifications AS yoy_change
    FROM h1b outt
      INNER JOIN (
      SELECT
        outt.SOC_NAME,
        COUNT(outt.SOC_NAME) AS 2013_certifications
      FROM h1b outt
      INNER JOIN (
        SELECT
          *
        FROM h1b
        WHERE
          CASE_STATUS LIKE 'CERTIFIED%'
      ) AS certified ON
        certified.ID = outt.ID
      WHERE
        outt.YEAR = 2013
      GROUP BY
        outt.SOC_NAME
    ) AS prev_yr ON
      prev_yr.SOC_NAME = outt.SOC_NAME
    WHERE
      outt.YEAR = 2014 AND
      outt.CASE_STATUS LIKE 'CERTIFIED%'
    GROUP BY
      outt.SOC_NAME,
      prev_yr.2013_certifications
    ORDER BY
      (COUNT(outt.SOC_NAME) - prev_yr.2013_certifications) DESC
  ''')
  # What were the highest growing roles for certified 
  # visa applications between 2014 and 2015?
  query8 = spark.sql('''
    SELECT
      outt.SOC_NAME,
      prev_yr.2014_certifications,
      COUNT(outt.SOC_NAME) AS 2015_certifications,
      COUNT(outt.SOC_NAME) - prev_yr.2014_certifications AS yoy_change
    FROM h1b outt
      INNER JOIN (
      SELECT
        outt.SOC_NAME,
        COUNT(outt.SOC_NAME) AS 2014_certifications
      FROM h1b outt
      INNER JOIN (
        SELECT
          *
        FROM h1b
        WHERE
          CASE_STATUS LIKE 'CERTIFIED%'
      ) AS certified ON
        certified.ID = outt.ID
      WHERE
        outt.YEAR = 2014
      GROUP BY
        outt.SOC_NAME
    ) AS prev_yr ON
      prev_yr.SOC_NAME = outt.SOC_NAME
    WHERE
      outt.YEAR = 2015 AND
      outt.CASE_STATUS LIKE 'CERTIFIED%'
    GROUP BY
      outt.SOC_NAME,
      prev_yr.2014_certifications
    ORDER BY
      (COUNT(outt.SOC_NAME) - prev_yr.2014_certifications) DESC
  ''')
  # What were the highest growing roles for certified 
  # visa applications between 2015 and 2016?
  query9 = spark.sql('''
    SELECT
      outt.SOC_NAME,
      prev_yr.2015_certifications,
      COUNT(outt.SOC_NAME) AS 2016_certifications,
      COUNT(outt.SOC_NAME) - prev_yr.2015_certifications AS yoy_change
    FROM h1b outt
      INNER JOIN (
      SELECT
        outt.SOC_NAME,
        COUNT(outt.SOC_NAME) AS 2015_certifications
      FROM h1b outt
      INNER JOIN (
        SELECT
          *
        FROM h1b
        WHERE
          CASE_STATUS LIKE 'CERTIFIED%'
      ) AS certified ON
        certified.ID = outt.ID
      WHERE
        outt.YEAR = 2015
      GROUP BY
        outt.SOC_NAME
    ) AS prev_yr ON
      prev_yr.SOC_NAME = outt.SOC_NAME
    WHERE
      outt.YEAR = 2016 AND
      outt.CASE_STATUS LIKE 'CERTIFIED%'
    GROUP BY
      outt.SOC_NAME,
      prev_yr.2015_certifications
    ORDER BY
      (COUNT(outt.SOC_NAME) - prev_yr.2015_certifications) DESC
  ''')
  # What were role categories which gained the most 
  # certifications in the 6 year span and how did 
  # they change year to year?
  query10 = spark.sql('''
    SELECT
      outt.SOC_NAME,
      res_2015.2011_certifications,
      res_2015.2012_certifications,
      res_2015.2013_certifications,
      res_2015.2014_certifications,
      res_2015.2015_certifications,
      COUNT(outt.SOC_NAME) AS 2016_certifications,
      (COUNT(outt.SOC_NAME) - res_2015.2011_certifications) AS net_change
    FROM h1b outt
    INNER JOIN (
      SELECT
        outt.SOC_NAME,
        res_2014.2011_certifications,
        res_2014.2012_certifications,
        res_2014.2013_certifications,
        res_2014.2014_certifications,
        COUNT(outt.SOC_NAME) AS 2015_certifications
      FROM h1b outt
      INNER JOIN (
        SELECT
          outt.SOC_NAME,
          res_2013.2011_certifications,
          res_2013.2012_certifications,
          res_2013.2013_certifications,
          COUNT(outt.SOC_NAME) AS 2014_certifications
        FROM h1b outt
        INNER JOIN (
          SELECT
            outt.SOC_NAME,
            res_2012.2011_certifications,
            res_2012.2012_certifications,
            COUNT(outt.SOC_NAME) AS 2013_certifications
          FROM h1b outt
          INNER JOIN (
            SELECT
              outt.SOC_NAME,
              res_2011.2011_certifications,
              COUNT(outt.SOC_NAME) AS 2012_certifications
            FROM h1b outt
              INNER JOIN (
              SELECT
                outt.SOC_NAME,
                COUNT(outt.SOC_NAME) AS 2011_certifications
              FROM h1b outt
              INNER JOIN (
                SELECT
                  *
                FROM h1b
                WHERE
                  CASE_STATUS LIKE 'CERTIFIED%'
              ) AS certified ON
                certified.ID = outt.ID
              WHERE
                outt.YEAR = 2011
              GROUP BY
                outt.SOC_NAME
            ) AS res_2011 ON
              res_2011.SOC_NAME = outt.SOC_NAME
            WHERE
              outt.YEAR = 2012 AND
              outt.CASE_STATUS LIKE 'CERTIFIED%'
            GROUP BY
              outt.SOC_NAME,
              res_2011.2011_certifications
          ) AS res_2012 ON
            res_2012.SOC_NAME = outt.SOC_NAME
          WHERE
            outt.YEAR = 2013 AND
            outt.CASE_STATUS LIKE 'CERTIFIED%'
          GROUP BY
            outt.SOC_NAME,
            res_2012.2011_certifications,
            res_2012.2012_certifications
        ) AS res_2013 ON
          res_2013.SOC_NAME = outt.SOC_NAME
        WHERE
          outt.YEAR = 2014 AND
          outt.CASE_STATUS LIKE 'CERTIFIED%'
        GROUP BY
          outt.SOC_NAME,
          res_2013.2011_certifications,
          res_2013.2012_certifications,
          res_2013.2013_certifications
      ) AS res_2014 ON
        res_2014.SOC_NAME = outt.SOC_NAME
      WHERE
        outt.YEAR = 2015 AND
        outt.CASE_STATUS LIKE 'CERTIFIED%'
      GROUP BY
        outt.SOC_NAME,
        res_2014.2011_certifications,
        res_2014.2012_certifications,
        res_2014.2013_certifications,
        res_2014.2014_certifications
    ) AS res_2015 ON
      res_2015.SOC_NAME = outt.SOC_NAME
    WHERE
      outt.YEAR = 2016 AND
      outt.CASE_STATUS LIKE 'CERTIFIED%'
    GROUP BY
      outt.SOC_NAME,
      res_2015.2011_certifications,
      res_2015.2012_certifications,
      res_2015.2013_certifications,
      res_2015.2014_certifications,
      res_2015.2015_certifications
    ORDER BY
      (COUNT(outt.SOC_NAME) - res_2015.2011_certifications) DESC
  ''')
  # What were jobs which gained the most 
  # certifications in the 6 year span and how did 
  # they change year to year?
  query11 = spark.sql('''
    SELECT
      outt.JOB_TITLE,
      res_2015.2011_certifications,
      res_2015.2012_certifications,
      res_2015.2013_certifications,
      res_2015.2014_certifications,
      res_2015.2015_certifications,
      COUNT(outt.JOB_TITLE) AS 2016_certifications,
      (COUNT(outt.JOB_TITLE) - res_2015.2011_certifications) AS net_change
    FROM h1b outt
    INNER JOIN (
      SELECT
        outt.JOB_TITLE,
        res_2014.2011_certifications,
        res_2014.2012_certifications,
        res_2014.2013_certifications,
        res_2014.2014_certifications,
        COUNT(outt.JOB_TITLE) AS 2015_certifications
      FROM h1b outt
      INNER JOIN (
        SELECT
          outt.JOB_TITLE,
          res_2013.2011_certifications,
          res_2013.2012_certifications,
          res_2013.2013_certifications,
          COUNT(outt.JOB_TITLE) AS 2014_certifications
        FROM h1b outt
        INNER JOIN (
          SELECT
            outt.JOB_TITLE,
            res_2012.2011_certifications,
            res_2012.2012_certifications,
            COUNT(outt.JOB_TITLE) AS 2013_certifications
          FROM h1b outt
          INNER JOIN (
            SELECT
              outt.JOB_TITLE,
              res_2011.2011_certifications,
              COUNT(outt.JOB_TITLE) AS 2012_certifications
            FROM h1b outt
              INNER JOIN (
              SELECT
                outt.JOB_TITLE,
                COUNT(outt.JOB_TITLE) AS 2011_certifications
              FROM h1b outt
              INNER JOIN (
                SELECT
                  *
                FROM h1b
                WHERE
                  CASE_STATUS LIKE 'CERTIFIED%'
              ) AS certified ON
                certified.ID = outt.ID
              WHERE
                outt.YEAR = 2011
              GROUP BY
                outt.JOB_TITLE
            ) AS res_2011 ON
              res_2011.JOB_TITLE = outt.JOB_TITLE
            WHERE
              outt.YEAR = 2012 AND
              outt.CASE_STATUS LIKE 'CERTIFIED%'
            GROUP BY
              outt.JOB_TITLE,
              res_2011.2011_certifications
          ) AS res_2012 ON
            res_2012.JOB_TITLE = outt.JOB_TITLE
          WHERE
            outt.YEAR = 2013 AND
            outt.CASE_STATUS LIKE 'CERTIFIED%'
          GROUP BY
            outt.JOB_TITLE,
            res_2012.2011_certifications,
            res_2012.2012_certifications
        ) AS res_2013 ON
          res_2013.JOB_TITLE = outt.JOB_TITLE
        WHERE
          outt.YEAR = 2014 AND
          outt.CASE_STATUS LIKE 'CERTIFIED%'
        GROUP BY
          outt.JOB_TITLE,
          res_2013.2011_certifications,
          res_2013.2012_certifications,
          res_2013.2013_certifications
      ) AS res_2014 ON
        res_2014.JOB_TITLE = outt.JOB_TITLE
      WHERE
        outt.YEAR = 2015 AND
        outt.CASE_STATUS LIKE 'CERTIFIED%'
      GROUP BY
        outt.JOB_TITLE,
        res_2014.2011_certifications,
        res_2014.2012_certifications,
        res_2014.2013_certifications,
        res_2014.2014_certifications
    ) AS res_2015 ON
      res_2015.JOB_TITLE = outt.JOB_TITLE
    WHERE
      outt.YEAR = 2016 AND
      outt.CASE_STATUS LIKE 'CERTIFIED%'
    GROUP BY
      outt.JOB_TITLE,
      res_2015.2011_certifications,
      res_2015.2012_certifications,
      res_2015.2013_certifications,
      res_2015.2014_certifications,
      res_2015.2015_certifications
    ORDER BY
      (COUNT(outt.JOB_TITLE) - res_2015.2011_certifications) DESC
  ''')
  # How are the visa certifications broken down by state?
  query12 = spark.sql('''
    SELECT
      outt.state,
      COUNT(outt.CASE_STATUS) AS visa_requests
    FROM h1b outt
    INNER JOIN (
      SELECT
        *
      FROM h1b
      WHERE
        CASE_STATUS LIKE 'CERTIFIED%'
    ) AS certified ON
      outt.ID = certified.ID
    GROUP BY
      outt.state
    ORDER BY
      COUNT(outt.CASE_STATUS) DESC
  ''')
  # How are the visa certifications broken down by city?
  query13 = spark.sql('''
    SELECT
      outt.WORKSITE,
      COUNT(outt.CASE_STATUS) AS visa_requests
    FROM h1b outt
    INNER JOIN (
      SELECT
        *
      FROM h1b
      WHERE
        CASE_STATUS LIKE 'CERTIFIED%'
    ) AS certified ON
      outt.ID = certified.ID
    GROUP BY
      outt.WORKSITE
    ORDER BY
      COUNT(outt.CASE_STATUS) DESC
  ''')
  # Are roles generally affected by full time classification?
  query14 = spark.sql('''
    SELECT
      outt.FULL_TIME_POSITION,
      COUNT(outt.CASE_STATUS) / inn.total_cases AS rate_certified
    FROM h1b outt
    INNER JOIN (
      SELECT
        FULL_TIME_POSITION,
        COUNT(CASE_STATUS) AS total_cases
      FROM h1b
      GROUP BY
        FULL_TIME_POSITION
    ) AS inn ON
      inn.FULL_TIME_POSITION = outt.FULL_TIME_POSITION
    WHERE
      outt.CASE_STATUS LIKE 'CERTIFIED%'
    GROUP BY
      outt.FULL_TIME_POSITION,
      inn.total_cases
    ORDER BY
      COUNT(outt.CASE_STATUS) / inn.total_cases DESC
  ''')
  # What are the average wages for denied visa petitions vs certified?
  query15 = spark.sql('''
    (
      SELECT
        COUNT(*) AS num_visas,
        SUM(PREVAILING_WAGE) / COUNT(PREVAILING_WAGE) AS avg_wage
      FROM h1b
      WHERE
        CASE_STATUS = 'DENIED'
    ) UNION (
      SELECT
        COUNT(*) AS num_visas,
        SUM(PREVAILING_WAGE) / COUNT(PREVAILING_WAGE) AS avg_wage
      FROM h1b
      WHERE
        CASE_STATUS LIKE 'CERTIFIED%'
    )
  ''')
  # Which jobs had the highest visa denial?
  query16 = spark.sql('''
    SELECT
      JOB_TITLE,
      COUNT(JOB_TITLE) AS visa_count
    FROM h1b
    WHERE
      CASE_STATUS = 'DENIED'
    GROUP BY
      JOB_TITLE
    ORDER BY
      COUNT(JOB_TITLE) DESC
  ''')
  # Which states had the highest visa denail ratio?
  query17 = spark.sql('''
    SELECT
      outt.state,
      COUNT(outt.state) / inn.total_visa_count AS denied_ratio
    FROM h1b outt
    INNER JOIN (
      SELECT
        state,
        COUNT(state) AS total_visa_count
      FROM h1b
      GROUP BY
        state
    ) AS inn ON
      inn.state = outt.state
    WHERE
      outt.CASE_STATUS = 'DENIED'
    GROUP BY
      outt.state,
      inn.total_visa_count
    ORDER BY
      COUNT(outt.state) / inn.total_visa_count DESC
  ''')
  # Which cities had the highest visa denial ratio?
  # Not very insightful as some cities had all of theirs denied
  query18 = spark.sql('''
    SELECT
      outt.WORKSITE,
      COUNT(outt.WORKSITE) / inn.total_visa_count AS denied_ratio
    FROM h1b outt
    INNER JOIN (
      SELECT
        WORKSITE,
        COUNT(WORKSITE) AS total_visa_count
      FROM h1b
      GROUP BY
        WORKSITE
    ) AS inn ON
      inn.WORKSITE = outt.WORKSITE
    WHERE
      outt.CASE_STATUS = 'DENIED'
    GROUP BY
      outt.WORKSITE,
      inn.total_visa_count
    ORDER BY
      COUNT(outt.WORKSITE) / inn.total_visa_count DESC
  ''')
  # Per state what was the highest applied job?
  query19 = spark.sql('''
    SELECT
      t2.*
    FROM (
      SELECT
        state,
        MAX(num_jobs) AS most_common_job
      FROM (
        SELECT
          state,
          SOC_NAME,
          COUNT(SOC_NAME) AS num_jobs
        FROM
          h1b
        GROUP BY
          state,
          SOC_NAME
      ) AS s1
      GROUP BY
        state
    ) AS w
    JOIN (
      SELECT
        state,
        SOC_NAME,
        COUNT(SOC_NAME) AS num_jobs
      FROM
        h1b
      GROUP BY
        state,
        SOC_NAME
    ) AS t2 ON 
      t2.state = w.state AND
      t2.num_jobs = w.most_common_job
    ORDER BY
      t2.num_jobs DESC,
      t2.state
  ''')
  # How many visa applications were withdrawn?
  query20 = spark.sql('''
    SELECT
      COUNT(*) AS withdrawn_visas
    FROM h1b
    WHERE
      CASE_STATUS LIKE '%WITHDRAWN'
  ''')
  # Which role categories had the most withdrawn visas?
  query21 = spark.sql('''
    SELECT
      SOC_NAME,
      COUNT(SOC_NAME) AS withdrawn_visas
    FROM h1b
    WHERE
      CASE_STATUS LIKE '%WITHDRAWN'
    GROUP BY
      SOC_NAME
    ORDER BY
      COUNT(SOC_NAME) DESC
  ''')
  # Which jobs had the most withdrawn visas?
  query22 = spark.sql('''
    SELECT
      JOB_TITLE,
      COUNT(JOB_TITLE) AS withdrawn_visas
    FROM h1b
    WHERE
      CASE_STATUS LIKE '%WITHDRAWN'
    GROUP BY
      JOB_TITLE
    ORDER BY
      COUNT(JOB_TITLE) DESC
  ''')
  # Print results
  print('\n### ADDITIONAL QUERIES ##########################\n')
  print('### QUERY 1 #####################################')
  query1.show(40)
  print('### QUERY 2 #####################################')
  query2.show(truncate=False)
  print('### QUERY 3 #####################################')
  query3.show(truncate=False)
  print('### QUERY 4 #####################################')
  query4.show(truncate=False)
  print('### QUERY 5 #####################################')
  query5.show(truncate=False)
  print('### QUERY 6 #####################################')
  query6.show(truncate=False)
  print('### QUERY 7 #####################################')
  query7.show(truncate=False)
  print('### QUERY 8 #####################################')
  query8.show(truncate=False)
  print('### QUERY 9 #####################################')
  query9.show(truncate=False)
  print('### QUERY 10 ####################################')
  query10.show(truncate=False)
  print('### QUERY 11 ####################################')
  query11.show(truncate=False)
  print('### QUERY 12 ####################################')
  query12.show(truncate=False)
  print('### QUERY 13 ####################################')
  query13.show(truncate=False)
  print('### QUERY 14 ####################################')
  query14.show(truncate=False)
  print('### QUERY 15 ####################################')
  query15.show(truncate=False)
  print('### QUERY 16 ####################################')
  query16.show(truncate=False)
  print('### QUERY 17 ####################################')
  query17.show(truncate=False)
  print('### QUERY 18 ####################################')
  query18.show(truncate=False)
  print('### QUERY 19 ####################################')
  query19.show(55, truncate=False)
  print('### QUERY 20 ####################################')
  query20.show(truncate=False)
  print('### QUERY 21 ####################################')
  query21.show(truncate=False)
  print('### QUERY 22 ####################################')
  query22.show(truncate=False)


# Determine the anser to the tasks posed in part 4
def tasks (df: pyspark.sql.DataFrame) -> None:
  # Allow the DataFrame to be used in Spark SQL queries
  df.createOrReplaceTempView('h1b')
  # What were the wages of denied applications compared
  # to accepted for the highest applied jobs?
  # -> The insight shows that higher salaries/incomplete
  #    applications lead to worse acceptance rates
  query_4a = spark.sql('''
    SELECT
      outt.SOC_NAME,
      inn.denied_visa_count,
      inn.avg_denied_wage,
      SUM(PREVAILING_WAGE) / COUNT(PREVAILING_WAGE) AS avg_certified_wage
    FROM h1b outt
    INNER JOIN (
      SELECT
        SOC_NAME,
        COUNT(SOC_NAME) AS denied_visa_count,
        SUM(PREVAILING_WAGE) / COUNT(PREVAILING_WAGE) AS avg_denied_wage
      FROM h1b
      WHERE
        CASE_STATUS = 'DENIED'
      GROUP BY
        SOC_NAME
    ) AS inn ON
      inn.SOC_NAME = outt.SOC_NAME
    WHERE
      outt.CASE_STATUS LIKE 'CERTIFIED%'
    GROUP BY
      outt.SOC_NAME,
      inn.denied_visa_count,
      inn.avg_denied_wage
    ORDER BY
      inn.denied_visa_count DESC
  ''')
  # How did wages change for role categories 
  # across the 6 year span?
  query_4bi = spark.sql('''
    SELECT
      outt.SOC_NAME,
      res_2015.2011_wages,
      res_2015.2012_wages,
      res_2015.2013_wages,
      res_2015.2014_wages,
      res_2015.2015_wages,
      SUM(outt.PREVAILING_WAGE) / COUNT(outt.PREVAILING_WAGE) AS 2016_wages,
      (SUM(outt.PREVAILING_WAGE) / COUNT(outt.PREVAILING_WAGE) - res_2015.2011_wages) AS net_change
    FROM h1b outt
    INNER JOIN (
      SELECT
        outt.SOC_NAME,
        res_2014.2011_wages,
        res_2014.2012_wages,
        res_2014.2013_wages,
        res_2014.2014_wages,
        SUM(outt.PREVAILING_WAGE) / COUNT(outt.PREVAILING_WAGE) AS 2015_wages
      FROM h1b outt
      INNER JOIN (
        SELECT
          outt.SOC_NAME,
          res_2013.2011_wages,
          res_2013.2012_wages,
          res_2013.2013_wages,
          SUM(outt.PREVAILING_WAGE) / COUNT(outt.PREVAILING_WAGE) AS 2014_wages
        FROM h1b outt
        INNER JOIN (
          SELECT
            outt.SOC_NAME,
            res_2012.2011_wages,
            res_2012.2012_wages,
            SUM(outt.PREVAILING_WAGE) / COUNT(outt.PREVAILING_WAGE) AS 2013_wages
          FROM h1b outt
          INNER JOIN (
            SELECT
              outt.SOC_NAME,
              res_2011.2011_wages,
              SUM(outt.PREVAILING_WAGE) / COUNT(outt.PREVAILING_WAGE) AS 2012_wages
            FROM h1b outt
              INNER JOIN (
              SELECT
                outt.SOC_NAME,
                SUM(outt.PREVAILING_WAGE) / COUNT(outt.PREVAILING_WAGE) AS 2011_wages
              FROM h1b outt
              INNER JOIN (
                SELECT
                  *
                FROM h1b
                WHERE
                  CASE_STATUS LIKE 'CERTIFIED%'
              ) AS certified ON
                certified.ID = outt.ID
              WHERE
                outt.YEAR = 2011
              GROUP BY
                outt.SOC_NAME
            ) AS res_2011 ON
              res_2011.SOC_NAME = outt.SOC_NAME
            WHERE
              outt.YEAR = 2012 AND
              outt.CASE_STATUS LIKE 'CERTIFIED%'
            GROUP BY
              outt.SOC_NAME,
              res_2011.2011_wages
          ) AS res_2012 ON
            res_2012.SOC_NAME = outt.SOC_NAME
          WHERE
            outt.YEAR = 2013 AND
            outt.CASE_STATUS LIKE 'CERTIFIED%'
          GROUP BY
            outt.SOC_NAME,
            res_2012.2011_wages,
            res_2012.2012_wages
        ) AS res_2013 ON
          res_2013.SOC_NAME = outt.SOC_NAME
        WHERE
          outt.YEAR = 2014 AND
          outt.CASE_STATUS LIKE 'CERTIFIED%'
        GROUP BY
          outt.SOC_NAME,
          res_2013.2011_wages,
          res_2013.2012_wages,
          res_2013.2013_wages
      ) AS res_2014 ON
        res_2014.SOC_NAME = outt.SOC_NAME
      WHERE
        outt.YEAR = 2015 AND
        outt.CASE_STATUS LIKE 'CERTIFIED%'
      GROUP BY
        outt.SOC_NAME,
        res_2014.2011_wages,
        res_2014.2012_wages,
        res_2014.2013_wages,
        res_2014.2014_wages
    ) AS res_2015 ON
      res_2015.SOC_NAME = outt.SOC_NAME
    WHERE
      outt.YEAR = 2016 AND
      outt.CASE_STATUS LIKE 'CERTIFIED%'
    GROUP BY
      outt.SOC_NAME,
      res_2015.2011_wages,
      res_2015.2012_wages,
      res_2015.2013_wages,
      res_2015.2014_wages,
      res_2015.2015_wages
    ORDER BY
      (SUM(outt.PREVAILING_WAGE) / COUNT(outt.PREVAILING_WAGE) - res_2015.2011_wages) DESC
  ''')
  # How did wages change for jobs 
  # across the 6 year span?
  query_4bii = spark.sql('''
    SELECT
      outt.JOB_TITLE,
      res_2015.2011_wages,
      res_2015.2012_wages,
      res_2015.2013_wages,
      res_2015.2014_wages,
      res_2015.2015_wages,
      SUM(outt.PREVAILING_WAGE) / COUNT(outt.PREVAILING_WAGE) AS 2016_wages,
      (SUM(outt.PREVAILING_WAGE) / COUNT(outt.PREVAILING_WAGE) - res_2015.2011_wages) AS net_change
    FROM h1b outt
    INNER JOIN (
      SELECT
        outt.JOB_TITLE,
        res_2014.2011_wages,
        res_2014.2012_wages,
        res_2014.2013_wages,
        res_2014.2014_wages,
        SUM(outt.PREVAILING_WAGE) / COUNT(outt.PREVAILING_WAGE) AS 2015_wages
      FROM h1b outt
      INNER JOIN (
        SELECT
          outt.JOB_TITLE,
          res_2013.2011_wages,
          res_2013.2012_wages,
          res_2013.2013_wages,
          SUM(outt.PREVAILING_WAGE) / COUNT(outt.PREVAILING_WAGE) AS 2014_wages
        FROM h1b outt
        INNER JOIN (
          SELECT
            outt.JOB_TITLE,
            res_2012.2011_wages,
            res_2012.2012_wages,
            SUM(outt.PREVAILING_WAGE) / COUNT(outt.PREVAILING_WAGE) AS 2013_wages
          FROM h1b outt
          INNER JOIN (
            SELECT
              outt.JOB_TITLE,
              res_2011.2011_wages,
              SUM(outt.PREVAILING_WAGE) / COUNT(outt.PREVAILING_WAGE) AS 2012_wages
            FROM h1b outt
              INNER JOIN (
              SELECT
                outt.JOB_TITLE,
                SUM(outt.PREVAILING_WAGE) / COUNT(outt.PREVAILING_WAGE) AS 2011_wages
              FROM h1b outt
              INNER JOIN (
                SELECT
                  *
                FROM h1b
                WHERE
                  CASE_STATUS LIKE 'CERTIFIED%'
              ) AS certified ON
                certified.ID = outt.ID
              WHERE
                outt.YEAR = 2011
              GROUP BY
                outt.JOB_TITLE
            ) AS res_2011 ON
              res_2011.JOB_TITLE = outt.JOB_TITLE
            WHERE
              outt.YEAR = 2012 AND
              outt.CASE_STATUS LIKE 'CERTIFIED%'
            GROUP BY
              outt.JOB_TITLE,
              res_2011.2011_wages
          ) AS res_2012 ON
            res_2012.JOB_TITLE = outt.JOB_TITLE
          WHERE
            outt.YEAR = 2013 AND
            outt.CASE_STATUS LIKE 'CERTIFIED%'
          GROUP BY
            outt.JOB_TITLE,
            res_2012.2011_wages,
            res_2012.2012_wages
        ) AS res_2013 ON
          res_2013.JOB_TITLE = outt.JOB_TITLE
        WHERE
          outt.YEAR = 2014 AND
          outt.CASE_STATUS LIKE 'CERTIFIED%'
        GROUP BY
          outt.JOB_TITLE,
          res_2013.2011_wages,
          res_2013.2012_wages,
          res_2013.2013_wages
      ) AS res_2014 ON
        res_2014.JOB_TITLE = outt.JOB_TITLE
      WHERE
        outt.YEAR = 2015 AND
        outt.CASE_STATUS LIKE 'CERTIFIED%'
      GROUP BY
        outt.JOB_TITLE,
        res_2014.2011_wages,
        res_2014.2012_wages,
        res_2014.2013_wages,
        res_2014.2014_wages
    ) AS res_2015 ON
      res_2015.JOB_TITLE = outt.JOB_TITLE
    WHERE
      outt.YEAR = 2016 AND
      outt.CASE_STATUS LIKE 'CERTIFIED%'
    GROUP BY
      outt.JOB_TITLE,
      res_2015.2011_wages,
      res_2015.2012_wages,
      res_2015.2013_wages,
      res_2015.2014_wages,
      res_2015.2015_wages
    ORDER BY
      (SUM(outt.PREVAILING_WAGE) / COUNT(outt.PREVAILING_WAGE) - res_2015.2011_wages) DESC
  ''')
  # Print the results
  print('\n### TASKS #######################################\n')
  print('### QUERY 4a ####################################')
  query_4a.show(truncate=False)
  print('### QUERY 4b ####################################')
  query_4bi.show(truncate=False)
  query_4bii.show(truncate=False)
  print('#################################################')


# Creates a PySpark Row object from an iterable of (year, certification_count)
def createRow(SOC_NAME: str, iterable: list) -> list:
  store = dict(iterable)
  return Row(
    SOC_NAME=SOC_NAME,
    certifications_2011=store.get(2011),
    certifications_2012=store.get(2012), 
    certifications_2013=store.get(2013),
    certifications_2014=store.get(2014),
    certifications_2015=store.get(2015), 
    certifications_2016=store.get(2016),
    net_change=store.get(2016) - store.get(2011) if store.get(2011) is not None and store.get(2016) is not None else 0
  )


# Times the speed of code execution in PySpark SQL vs RDD manipulations
def timing (df: pyspark.sql.DataFrame) -> None:
  # Allow the DataFrame to be used in Spark SQL queries
  df.createOrReplaceTempView('h1b')
  # Time a complicated query with printing
  start_time_1 = time()
  dfquery = spark.sql('''
    SELECT
      outt.SOC_NAME,
      res_2015.2011_certifications,
      res_2015.2012_certifications,
      res_2015.2013_certifications,
      res_2015.2014_certifications,
      res_2015.2015_certifications,
      COUNT(outt.SOC_NAME) AS 2016_certifications,
      (COUNT(outt.SOC_NAME) - res_2015.2011_certifications) AS net_change
    FROM h1b outt
    INNER JOIN (
      SELECT
        outt.SOC_NAME,
        res_2014.2011_certifications,
        res_2014.2012_certifications,
        res_2014.2013_certifications,
        res_2014.2014_certifications,
        COUNT(outt.SOC_NAME) AS 2015_certifications
      FROM h1b outt
      INNER JOIN (
        SELECT
          outt.SOC_NAME,
          res_2013.2011_certifications,
          res_2013.2012_certifications,
          res_2013.2013_certifications,
          COUNT(outt.SOC_NAME) AS 2014_certifications
        FROM h1b outt
        INNER JOIN (
          SELECT
            outt.SOC_NAME,
            res_2012.2011_certifications,
            res_2012.2012_certifications,
            COUNT(outt.SOC_NAME) AS 2013_certifications
          FROM h1b outt
          INNER JOIN (
            SELECT
              outt.SOC_NAME,
              res_2011.2011_certifications,
              COUNT(outt.SOC_NAME) AS 2012_certifications
            FROM h1b outt
              INNER JOIN (
              SELECT
                outt.SOC_NAME,
                COUNT(outt.SOC_NAME) AS 2011_certifications
              FROM h1b outt
              INNER JOIN (
                SELECT
                  *
                FROM h1b
                WHERE
                  CASE_STATUS LIKE 'CERTIFIED%'
              ) AS certified ON
                certified.ID = outt.ID
              WHERE
                outt.YEAR = 2011
              GROUP BY
                outt.SOC_NAME
            ) AS res_2011 ON
              res_2011.SOC_NAME = outt.SOC_NAME
            WHERE
              outt.YEAR = 2012 AND
              outt.CASE_STATUS LIKE 'CERTIFIED%'
            GROUP BY
              outt.SOC_NAME,
              res_2011.2011_certifications
          ) AS res_2012 ON
            res_2012.SOC_NAME = outt.SOC_NAME
          WHERE
            outt.YEAR = 2013 AND
            outt.CASE_STATUS LIKE 'CERTIFIED%'
          GROUP BY
            outt.SOC_NAME,
            res_2012.2011_certifications,
            res_2012.2012_certifications
        ) AS res_2013 ON
          res_2013.SOC_NAME = outt.SOC_NAME
        WHERE
          outt.YEAR = 2014 AND
          outt.CASE_STATUS LIKE 'CERTIFIED%'
        GROUP BY
          outt.SOC_NAME,
          res_2013.2011_certifications,
          res_2013.2012_certifications,
          res_2013.2013_certifications
      ) AS res_2014 ON
        res_2014.SOC_NAME = outt.SOC_NAME
      WHERE
        outt.YEAR = 2015 AND
        outt.CASE_STATUS LIKE 'CERTIFIED%'
      GROUP BY
        outt.SOC_NAME,
        res_2014.2011_certifications,
        res_2014.2012_certifications,
        res_2014.2013_certifications,
        res_2014.2014_certifications
    ) AS res_2015 ON
      res_2015.SOC_NAME = outt.SOC_NAME
    WHERE
      outt.YEAR = 2016 AND
      outt.CASE_STATUS LIKE 'CERTIFIED%'
    GROUP BY
      outt.SOC_NAME,
      res_2015.2011_certifications,
      res_2015.2012_certifications,
      res_2015.2013_certifications,
      res_2015.2014_certifications,
      res_2015.2015_certifications
    ORDER BY
      (COUNT(outt.SOC_NAME) - res_2015.2011_certifications) DESC
  ''')
  print('\n### TIMING ######################################\n')
  dfquery.show(truncate=False)
  end_time_1 = time()
  # Skip the conversion time to rdd and only count complex operations
  rdd = df.rdd
  start_time_2 = time()
  rdd = rdd.filter(lambda row: row.CASE_STATUS is not None and row.SOC_NAME is not None and 'CERTIFIED' in row.CASE_STATUS) \
    .map(lambda row: ((row.SOC_NAME, row.YEAR), 1)).reduceByKey(lambda v1, v2: v1 + v2) \
      .map(lambda item: (item[0][0], (item[0][1], item[1]))).groupByKey() \
        .map(lambda item: createRow(item[0], [elem for elem in item[1]])).sortBy(lambda row: row.net_change, ascending=False)
  rdd_df = spark.createDataFrame(rdd)
  rdd_df.show(truncate=False)
  end_time_2 = time()
  print_strs = list()
  print_strs.append(f'Test 1: Spark DataFrame SQL completed in {end_time_1 - start_time_1} seconds, RDD operations completed in {end_time_2 - start_time_2} seconds.')
  # Time a less complicated query with no printing for the rdd 
  # (still taking 20 records so execution happens)
  start_time_1 = time()
  dfquery2 = spark.sql('''
    SELECT
      SOC_NAME,
      COUNT(SOC_NAME) AS number_certified_jobs
    FROM h1b
    WHERE
      CASE_STATUS LIKE 'CERTIFIED%'
    GROUP BY
      SOC_NAME
    ORDER BY
      COUNT(SOC_NAME) DESC
  ''')
  dfquery2.show(truncate=False)
  end_time_1 = time()
  rdd = df.rdd
  start_time_2 = time()
  rdd = rdd.filter(lambda row: row.CASE_STATUS is not None and 'CERTIFIED' in row.CASE_STATUS).map(lambda row: (row.SOC_NAME, 1)) \
    .reduceByKey(lambda v1, v2: v1 + v2).sortBy(lambda item: item[1], ascending=False)
  rdd.take(20)
  end_time_2 = time()
  print_strs.append(f'Test 2: Spark DataFrame SQL completed in {end_time_1 - start_time_1} seconds, RDD operations completed in {end_time_2 - start_time_2} seconds.')
  # Timing very simple query with no printing for either
  start_time_1 = time()
  dfquery3 = spark.sql('''
    SELECT
      *
    FROM h1b
    WHERE
      YEAR = 2011
  ''')
  end_time_1 = time()
  rdd = df.rdd
  start_time_2 = time()
  rdd = rdd.filter(lambda row: row.YEAR is not None and row.YEAR == 2011)
  end_time_2 = time()
  print_strs.append(f'Test 3: Spark DataFrame SQL completed in {end_time_1 - start_time_1} seconds, RDD operations completed in {end_time_2 - start_time_2} seconds.')
  # Print results
  for s in print_strs:
    print(s)


def main () -> None:
  dataset_name = args.dataset if args is not None else 'FinalProject'
  table_name = args.table if args is not None else 'h1b'
  default_hdfs = '/user/Nicolas/project/parquet'
  hdfs_directory = f'hdfs://{hdfs_address}{args.hdfs if args is not None else default_hdfs}'
  loadDataTableFromGoogleStorage(dataset_name, table_name)
  loadBigQueryToHdfs(hdfs_directory, dataset_name, table_name)
  df = loadFromHdfs(hdfs_directory)
  if args is None or not args.no_basic:
    basicQueries(df)
  if args is None or not args.no_additional:
    additionalQueries(df)
  if args is None or not args.no_task:
    tasks(df)
  if args is None or not args.no_timing:
    timing(df)


if __name__ == '__main__':
  main()