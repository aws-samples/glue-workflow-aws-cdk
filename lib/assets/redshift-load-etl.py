import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

job_args = getResolvedOptions(sys.argv, [
        'glue_database_name',
        'glue_input_file1',
        'output_bucket_name',
        'TempDir'
        ])

glue_db = job_args['glue_database_name']
glue_table1 = job_args['glue_input_file1'].replace('.','_').replace('-','_')
bucket = job_args['output_bucket_name']
tempDir = job_args['TempDir']

glueContext = GlueContext(SparkContext.getOrCreate())

job = Job(glueContext)

cc_frame = glueContext.create_dynamic_frame.from_catalog(database = glue_db, table_name = "output_data", redshift_tmp_dir = tempDir, transformation_ctx = "cc_frame")

cc_redshift = glueContext.write_dynamic_frame.from_jdbc_conf(frame = cc_frame, catalog_connection = "redshift-connect", connection_options = {"dbtable": "blockchain-covid-table", "database": "db-blockchain-covid"}, redshift_tmp_dir = tempDir, transformation_ctx = "cc_redshift")

job.commit()

