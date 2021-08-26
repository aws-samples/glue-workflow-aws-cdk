# %pyspark
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

job_args = getResolvedOptions(sys.argv, [
        'glue_database_name',
        'glue_input_file1',
        'glue_input_file2',
        'glue_input_file3',
        'glue_input_file4',
        'output_bucket_name'
        ])

glue_db = job_args['glue_database_name']
glue_table1 = job_args['glue_input_file1'].replace('.','_').replace('-','_')
glue_table2 = job_args['glue_input_file2'].replace('.','_').replace('-','_')
glue_table3 = job_args['glue_input_file3'].replace('.','_').replace('-','_')
glue_table4 = job_args['glue_input_file4'].replace('.','_').replace('-','_')
bucket = job_args['output_bucket_name']

spark = SparkSession.builder.getOrCreate()
glueContext = GlueContext(SparkContext.getOrCreate())

bitcoin_dyf = glueContext.create_dynamic_frame.from_catalog(
      database = glue_db,
      table_name = glue_table1)
bitcoin_dyf.printSchema()

ethereum_dyf = glueContext.create_dynamic_frame.from_catalog(
      database = glue_db,
      table_name = glue_table2)
ethereum_dyf.printSchema()

covid_hiring_dyf = glueContext.create_dynamic_frame.from_catalog(
      database = glue_db,
      table_name = glue_table3)
covid_hiring_dyf.printSchema()

covid_cases_dyf = glueContext.create_dynamic_frame.from_catalog(
      database = glue_db,
      table_name = glue_table4)
covid_cases_dyf.printSchema()


##### ETL #####

# Join Covid case data & Covid hiring data
covid_hiring_cases = Join.apply(covid_hiring_dyf, covid_cases_dyf, "post_date", "date")

# Join Bitcoin and Ethereum data
btc_eth = Join.apply(bitcoin_dyf, ethereum_dyf, "col0", "col0")

# After the join, replace ".col" with "btc" to help with mapping data types and renaming columns later
clean_btc_eth = btc_eth.toDF()
clean_btc_eth_df = clean_btc_eth.toDF(*(c.replace('.', 'btc') for c in clean_btc_eth.columns))

# convert the joined crpyto datasets back to Dynamic Frame
clean_btc_eth_dyf = DynamicFrame.fromDF(clean_btc_eth_df, glueContext, "crypto_columns")
covid_hiring_cases_df = covid_hiring_cases.toDF()

# Filter unwanted values out of the joined dataset                     
btc_eth_filter = Filter.apply(frame = clean_btc_eth_dyf,
                            f = lambda x: x["col0"] not in ["Date"] and x["col1"] not in ["Price"] and x["col2"] not in ["Open"] and x["col3"] not in ["High"] and x["col4"] not in ["Low"] and x["col5"] not in ["Vol"] and x["col6"] not in ["Change %"] and ["btccol0"] not in ["Date"] and x["btccol1"] not in ["Price"] and x["btccol2"] not in ["Open"] and x["btccol3"] not in ["High"] and x["btccol4"] not in ["Low"] and x["btccol5"] not in ["Vol"] and x["btccol6"] not in ["Change %"])

# Change date format for crypto date in order to join to covid dates
btc_eth_df = btc_eth_filter.toDF().withColumn("col0",from_unixtime(unix_timestamp(col("col0"),'dd-MMM-yy'),'yyyy-MM-dd'))


# Convert both bitcoin & ethereum / covid hiring & covid cases datasets back to DynamicFrame
covid = DynamicFrame.fromDF(covid_hiring_cases_df, glueContext, "covid")
crypto = DynamicFrame.fromDF(btc_eth_df, glueContext, "crypto")

# Join the datasets together, resulting in 4 datasets joined as a single table now. 
cc_join = Join.apply(covid, crypto, 'date', 'col0')
cc_join_df = cc_join.toDF().withColumn('btccol1', regexp_replace('btccol1', ',', '')).withColumn('btccol2', regexp_replace('btccol2', ',', '')).withColumn('btccol3', regexp_replace('btccol3', ',', '')).withColumn('btccol4', regexp_replace('btccol4', ',', '')).withColumn('col1', regexp_replace('col1', ',', '')).withColumn('col3', regexp_replace('col2', ',', '')).withColumn('col2', regexp_replace('col3', ',', '')).withColumn('col4', regexp_replace('col4', ',', ''))
cc_join_df.createOrReplaceTempView('crypto_covid')

# Create final dataset with SQL

# Use SQL to rename columns and change data types
cc_sql = spark.sql('SELECT date(date) as cc_date, double(btccol1) as btc_price, double(btccol2) as btc_open, double(btccol3) as btc_high, double(btccol4) as btc_low, double(col1) as eth_price, double(col2) as eth_open, double(col3) as eth_high, double(col4) as eth_low, btccol6 as btc_change, col6 as eth_change, total_cases, new_cases, count_id_indexed as hiring_avg FROM crypto_covid')
cc_dyf = DynamicFrame.fromDF(cc_sql, glueContext, "cc")
cc_parquet = glueContext.write_dynamic_frame.from_options(frame = cc_dyf, connection_type = "s3", format = "glueparquet", connection_options = {"path": "s3://" +bucket+ "/output-data/", "compression": "snappy", "partitionKeys": [], 'groupFiles': 'inPartition', 'groupSize': '10485760'}, transformation_ctx = "cc_parquet")
