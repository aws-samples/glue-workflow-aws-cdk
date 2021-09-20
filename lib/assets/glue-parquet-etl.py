# %pyspark

# // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# // SPDX-License-Identifier: MIT-0

import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re

job_args = getResolvedOptions(sys.argv, [
        'glue_database_name',
        'glue_covid_table',
        'glue_hiring_table',
        'output_bucket_name',
        'output_prefix_path',
        'JOB_NAME'
        ])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(job_args['JOB_NAME'], job_args)

glue_db = job_args['glue_database_name']
glue_covid_table = job_args['glue_covid_table']
glue_hiring_table = job_args['glue_hiring_table']
bucket = job_args['output_bucket_name']
parquet_prefix = job_args['output_prefix_path']

s3 = boto3.client('s3')

# Remove prior downloads before pulling new data
objs = s3.list_objects_v2(Bucket=bucket, Prefix='processed-data/')
if 'Contents' in objs:
        for obj in objs['Contents']:
                print('Deleting: ', obj['Key'])
                s3.delete_object(Bucket=bucket, Key=obj['Key'])



DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = glue_db, table_name = glue_hiring_table, transformation_ctx = "DataSource0")

DataSource1 = glueContext.create_dynamic_frame.from_catalog(database = glue_db, table_name = glue_covid_table, transformation_ctx = "DataSource1")

# Join Covid case data with hiring data
Transform0 = Join.apply(frame1 = DataSource1, frame2 = DataSource0, keys2 = ["post_date"], keys1 = ["date"], transformation_ctx = "Transform0")

Transform1 = ApplyMapping.apply(frame = Transform0, mappings = [("iso_code", "string", "iso_code", "string"), ("continent", "string", "continent", "string"), ("location", "string", "location", "string"), ("date", "string", "date", "string"), ("total_cases", "double", "total_cases", "double"), ("new_cases", "double", "new_cases", "double"), ("new_cases_smoothed", "double", "new_cases_smoothed", "double"), ("total_deaths", "double", "total_deaths", "double"), ("new_deaths", "double", "new_deaths", "double"), ("new_deaths_smoothed", "double", "new_deaths_smoothed", "double"), ("total_cases_per_million", "double", "total_cases_per_million", "double"), ("new_cases_per_million", "double", "new_cases_per_million", "double"), ("new_cases_smoothed_per_million", "double", "new_cases_smoothed_per_million", "double"), ("total_deaths_per_million", "double", "total_deaths_per_million", "double"), ("new_deaths_per_million", "double", "new_deaths_per_million", "double"), ("new_deaths_smoothed_per_million", "double", "new_deaths_smoothed_per_million", "double"), ("reproduction_rate", "double", "reproduction_rate", "double"), ("icu_patients", "string", "icu_patients", "string"), ("icu_patients_per_million", "string", "icu_patients_per_million", "string"), ("hosp_patients", "string", "hosp_patients", "string"), ("hosp_patients_per_million", "string", "hosp_patients_per_million", "string"), ("weekly_icu_admissions", "string", "weekly_icu_admissions", "string"), ("weekly_icu_admissions_per_million", "string", "weekly_icu_admissions_per_million", "string"), ("weekly_hosp_admissions", "string", "weekly_hosp_admissions", "string"), ("weekly_hosp_admissions_per_million", "string", "weekly_hosp_admissions_per_million", "string"), ("new_tests", "string", "new_tests", "string"), ("total_tests", "string", "total_tests", "string"), ("total_tests_per_thousand", "string", "total_tests_per_thousand", "string"), ("new_tests_per_thousand", "string", "new_tests_per_thousand", "string"), ("new_tests_smoothed", "string", "new_tests_smoothed", "string"), ("new_tests_smoothed_per_thousand", "string", "new_tests_smoothed_per_thousand", "string"), ("positive_rate", "string", "positive_rate", "string"), ("tests_per_case", "string", "tests_per_case", "string"), ("tests_units", "string", "tests_units", "string"), ("total_vaccinations", "double", "total_vaccinations", "double"), ("people_vaccinated", "double", "people_vaccinated", "double"), ("people_fully_vaccinated", "double", "people_fully_vaccinated", "double"), ("total_boosters", "string", "total_boosters", "string"), ("new_vaccinations", "double", "new_vaccinations", "double"), ("new_vaccinations_smoothed", "double", "new_vaccinations_smoothed", "double"), ("total_vaccinations_per_hundred", "double", "total_vaccinations_per_hundred", "double"), ("people_vaccinated_per_hundred", "double", "people_vaccinated_per_hundred", "double"), ("people_fully_vaccinated_per_hundred", "double", "people_fully_vaccinated_per_hundred", "double"), ("total_boosters_per_hundred", "string", "total_boosters_per_hundred", "string"), ("new_vaccinations_smoothed_per_million", "double", "new_vaccinations_smoothed_per_million", "double"), ("stringency_index", "double", "stringency_index", "double"), ("population", "double", "population", "double"), ("population_density", "double", "population_density", "double"), ("median_age", "double", "median_age", "double"), ("aged_65_older", "double", "aged_65_older", "double"), ("aged_70_older", "double", "aged_70_older", "double"), ("gdp_per_capita", "double", "gdp_per_capita", "double"), ("extreme_poverty", "string", "extreme_poverty", "string"), ("cardiovasc_death_rate", "double", "cardiovasc_death_rate", "double"), ("diabetes_prevalence", "double", "diabetes_prevalence", "double"), ("female_smokers", "string", "female_smokers", "string"), ("male_smokers", "string", "male_smokers", "string"), ("handwashing_facilities", "double", "handwashing_facilities", "double"), ("hospital_beds_per_thousand", "double", "hospital_beds_per_thousand", "double"), ("life_expectancy", "double", "life_expectancy", "double"), ("human_development_index", "double", "human_development_index", "double"), ("excess_mortality", "string", "excess_mortality", "string"), ("post_date", "string", "post_date", "string"), ("count_id_indexed", "double", "count_id_indexed", "double")], transformation_ctx = "Transform1")

# Filter only on data in USA
Transform2 = Filter.apply(frame = Transform1, f = lambda x: x["iso_code"] in ["USA"], transformation_ctx = "Transform2")

DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform2, format_options = {"compression": "snappy"}, connection_type = "s3", format = "glueparquet", connection_options = {"path": "s3://" + bucket + parquet_prefix, "partitionKeys": []}, transformation_ctx = "DataSink0")
job.commit()