# // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# // SPDX-License-Identifier: MIT-0

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
        'output_bucket_name',
        'TempDir'
        ])

glue_db = job_args['glue_database_name']
bucket = job_args['output_bucket_name']
tempDir = job_args['TempDir']

glueContext = GlueContext(SparkContext.getOrCreate())

job = Job(glueContext)

# load parquet data into dynamic frame
covid_hiring_dyf = glueContext.create_dynamic_frame.from_catalog(database = glue_db, table_name = "processed_data", redshift_tmp_dir = tempDir, transformation_ctx = "covid_hiring_dyf")

# write data from S3 into Redshift 
redshift_load_dyf = glueContext.write_dynamic_frame.from_jdbc_conf(frame = covid_hiring_dyf, catalog_connection = "redshift-connect", connection_options = {"preactions":"truncate table covid_hiring_table;", "dbtable": "covid_hiring_table", "database": "db-covid-hiring"}, redshift_tmp_dir = tempDir, transformation_ctx = "redshift_load_dyf")

job.commit()




