# // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# // SPDX-License-Identifier: MIT-0

import boto3
import sys
from awsglue.utils import getResolvedOptions

job_args = getResolvedOptions(sys.argv, [
        'source_BucketName',
        'target_BucketName',
        'obj_1_source_key',
        'obj_1_target_key',
        'obj_2_source_key',
        'obj_2_target_key',
        'obj_3_source_key',
        'obj_3_target_key',
        'obj_4_source_key',
        'obj_4_target_key',
        'obj_5_source_key',
        'obj_5_target_key',
        'obj_6_source_key',
        'obj_6_target_key',
        'obj_7_source_key',
        'obj_7_target_key'])

s3 = boto3.client('s3')

result=s3.copy_object(Bucket=job_args['target_BucketName'],Key=job_args['obj_1_target_key'],CopySource={'Bucket':job_args['source_BucketName'],'Key':job_args['obj_1_source_key']})
print(result)
result=s3.copy_object(Bucket=job_args['target_BucketName'],Key=job_args['obj_2_target_key'],CopySource={'Bucket':job_args['source_BucketName'],'Key':job_args['obj_2_source_key']})
print(result)
result=s3.copy_object(Bucket=job_args['target_BucketName'],Key=job_args['obj_3_target_key'],CopySource={'Bucket':job_args['source_BucketName'],'Key':job_args['obj_3_source_key']})
print(result)
result=s3.copy_object(Bucket=job_args['target_BucketName'],Key=job_args['obj_4_target_key'],CopySource={'Bucket':job_args['source_BucketName'],'Key':job_args['obj_4_source_key']})
print(result)
result=s3.copy_object(Bucket=job_args['target_BucketName'],Key=job_args['obj_5_target_key'],CopySource={'Bucket':job_args['source_BucketName'],'Key':job_args['obj_5_source_key']})
print(result)
result=s3.copy_object(Bucket=job_args['target_BucketName'],Key=job_args['obj_6_target_key'],CopySource={'Bucket':job_args['source_BucketName'],'Key':job_args['obj_6_source_key']})
print(result)
result=s3.copy_object(Bucket=job_args['target_BucketName'],Key=job_args['obj_7_target_key'],CopySource={'Bucket':job_args['source_BucketName'],'Key':job_args['obj_7_source_key']})
print(result)
