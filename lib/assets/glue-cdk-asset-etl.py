# // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# // SPDX-License-Identifier: MIT-0

import boto3
import sys
from awsglue.utils import getResolvedOptions

job_args = getResolvedOptions(sys.argv, [
        'source_BucketName',
        'target_BucketName',
        'target_covidPrefix',
        'target_hiringPrefix',
        'covid_source_bucket',
        'obj_covid_source_key',
        'obj_covid_target_key',
        'hiring_source_bucket',
        'obj_hiring_source_key',
        'obj_hiring_target_key',
        'obj_1_source_key',
        'obj_1_target_key',
        'obj_2_source_key',
        'obj_2_target_key',
        'obj_3_source_key',
        'obj_3_target_key',
       ])


s3 = boto3.client('s3')

# Remove prior downloads before pulling new data
objs = s3.list_objects_v2(Bucket=job_args['target_BucketName'], Prefix=job_args['target_covidPrefix'])
if 'Contents' in objs:
        for obj in objs['Contents']:
                print('Deleting: ', obj['Key'])
                s3.delete_object(Bucket=job_args['target_BucketName'], Key=obj['Key'])

objs2 = s3.list_objects_v2(Bucket=job_args['target_BucketName'], Prefix=job_args['target_hiringPrefix'])
if 'Contents' in objs2:
        for obj in objs2['Contents']:
                print('Deleting: ', obj['Key'])
                s3.delete_object(Bucket=job_args['target_BucketName'], Key=obj['Key'])

# Pull new data
result=s3.copy_object(Bucket=job_args['target_BucketName'],Key=job_args['obj_covid_target_key'],CopySource={'Bucket':job_args['covid_source_bucket'],'Key':job_args['obj_covid_source_key']})
print(result)
result=s3.copy_object(Bucket=job_args['target_BucketName'],Key=job_args['obj_hiring_target_key'],CopySource={'Bucket':job_args['hiring_source_bucket'],'Key':job_args['obj_hiring_source_key']})
print(result)
result=s3.copy_object(Bucket=job_args['target_BucketName'],Key=job_args['obj_1_target_key'],CopySource={'Bucket':job_args['source_BucketName'],'Key':job_args['obj_1_source_key']})
print(result)
result=s3.copy_object(Bucket=job_args['target_BucketName'],Key=job_args['obj_2_target_key'],CopySource={'Bucket':job_args['source_BucketName'],'Key':job_args['obj_2_source_key']})
print(result)
result=s3.copy_object(Bucket=job_args['target_BucketName'],Key=job_args['obj_3_target_key'],CopySource={'Bucket':job_args['source_BucketName'],'Key':job_args['obj_3_source_key']})
print(result)

