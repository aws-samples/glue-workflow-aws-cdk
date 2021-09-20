// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

import * as cdk from "@aws-cdk/core";
import * as glue from "@aws-cdk/aws-glue";
import { Asset } from "@aws-cdk/aws-s3-assets";
import {
  Role,
  ManagedPolicy,
  ServicePrincipal,
  Policy,
  PolicyStatement,
  Effect,
} from "@aws-cdk/aws-iam";
import * as path from "path";

//define meaningful s3 object keys for copied assets and input CSV files
const scriptsPath = "scripts/";
const covidPath = "covid-data/";
const hiringPath = "hiring-data/";
const covidCasesTable = "covid_data";
const covidHiringTable = "hiring_data"
const parquetPath = "/processed-data/"
const covid_src_bucket = "covid19-lake";
const covid_src_key = "rearc-covid-19-world-cases-deaths-testing/csv/covid-19-world-cases-deaths-testing.csv";
const hiring_src_bucket = "greenwichhr-covidjobimpacts";
const hiring_src_key = "overall.csv.part_00000";
const obj_covidCases = "covid_cases.csv";
const obj_covidHiring = "covid_hiring.csv";
const obj_assets = "glue-cdk-asset-etl.py";
const obj_etl = "glue-parquet-etl.py";
const obj_redshiftLoad = "redshift-load-etl.py";

//set AWS managed policy arn and glue service URL
const glue_managed_policy =
  "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole";
const glue_ServiceUrl = "glue.amazonaws.com";

export class BlogGlueWorkFlowStack extends cdk.Stack {
  public readonly glueRole: Role;

  constructor(scope: cdk.App, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    //###  Add assets to S3 bucket as individual files  #####

    //python scripts run in Glue Workflow
    const f_pyAssetETL = new Asset(this, "py-asset-etl", {
      path: path.join(__dirname, "assets/glue-cdk-asset-etl.py"),
    });
    const f_pyParquet = new Asset(this, "py-load", {
      path: path.join(__dirname, "assets/glue-parquet-etl.py"),
    });
    const f_pyRedshiftLoad = new Asset(this, "redshift-load", {
      path: path.join(__dirname, "assets/redshift-load-etl.py"),
    });

    //Get dynamic CDK asset bucket name to pass into Glue Jobs
    const assetBucketName = f_pyAssetETL.s3BucketName;

    //create glue database
    const glue_db = new glue.Database(this, "glue-workflow-db", {
      databaseName: "glue-workflow-db",
    });

    //create glue cralwer role to access S3 bucket
    const glue_crawler_role = new Role(this, "glue-crawler-role", {
      roleName: "AWSGlueServiceRole-AccessS3Bucket",
      description:
        "Assigns the managed policy AWSGlueServiceRole to AWS Glue Crawler so it can crawl S3 buckets",
      managedPolicies: [
        ManagedPolicy.fromManagedPolicyArn(
          this,
          "glue-service-policy",
          glue_managed_policy
        ),
      ],
      assumedBy: new ServicePrincipal(glue_ServiceUrl),
    });
    this.glueRole = glue_crawler_role;

    //add policy to role to grant access to S3 asset bucket and public buckets
    const iam_policy_forAssets = new Policy(this, "iam-policy-forAssets", {
      force: true,
      policyName: "glue-policy-workflowAssetAccess",
      roles: [glue_crawler_role],
      statements: [
        new PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject",
            "s3:ListBucket",
          ],
          resources: ["arn:aws:s3:::" + f_pyAssetETL.s3BucketName + "/*"],
        }),
        new PolicyStatement({
          effect: Effect.ALLOW,
          actions: ["s3:GetObject"],
          resources: [
            "arn:aws:s3:::" + covid_src_bucket + "/*",
            "arn:aws:s3:::" + hiring_src_bucket + "/*",
          ],
        }),
      ],
    });

    //Define paths for scripts and data
    const scripts = "s3://" + assetBucketName + "/" + scriptsPath;
    const covid = "s3://" + assetBucketName + "/" + covidPath;
    const hiring = "s3://" + assetBucketName + "/" + hiringPath;
    const redshift_temp_dir = "s3://" + assetBucketName + "/output/temp/";
    const outputPath = "s3://" + assetBucketName + parquetPath;

    //create glue crawler to crawl csv files in S3
    const glue_crawler_s3 = new glue.CfnCrawler(this, "glue-crawler-s3", {
      name: "s3-csv-crawler",
      role: glue_crawler_role.roleName,
      targets: {
        s3Targets: [
          {
            path: covid
          },
          {
            path: hiring
          },
        ],
      },
      databaseName: glue_db.databaseName,
      schemaChangePolicy: {
        updateBehavior: "UPDATE_IN_DATABASE",
        deleteBehavior: "DEPRECATE_IN_DATABASE",
      },
    });

    //create glue crawler to crawl parqet files in S3
    const glue_crawler_s3_parquet = new glue.CfnCrawler(
      this,
      "glue-crawler-s3-parquet",
      {
        name: "s3-parquet-crawler",
        role: glue_crawler_role.roleName,
        targets: {
          s3Targets: [
            {
              path: outputPath,
            },
          ],
        },
        databaseName: glue_db.databaseName,
        schemaChangePolicy: {
          updateBehavior: "UPDATE_IN_DATABASE",
          deleteBehavior: "DEPRECATE_IN_DATABASE",
        },
      }
    );

    //####  Create the glue workflow, jobs and triggers that will handle the ETL to convert CSV to Parquet and load the parquet file into Redshift #####

    //create glue workflow
    const glue_workflow = new glue.CfnWorkflow(this, "glue-workflow", {
      name: "glue-workflow",
      description:
        "ETL workflow to convert CSV to parquet and then load into Redshift",
    });

    //create jobs
    const glue_job_asset = new glue.CfnJob(this, "glue-job-asset", {
      name: "glue-workflow-assetjob",
      description: "Copy CDK assets to scripts folder and give meaningful name",
      role: glue_crawler_role.roleArn,
      executionProperty: {
        maxConcurrentRuns: 1,
      },
      command: {
        name: "glueetl",
        pythonVersion: "3",
        scriptLocation: f_pyAssetETL.s3ObjectUrl,
      },
      defaultArguments: {
        "--TempDir": "s3://" + f_pyAssetETL.s3BucketName + "/output/temp/",
        "--job-bookmark-option": "job-bookmark-disable",
        "--job-language": "python",
        "--spark-event-logs-path":
          "s3://" + f_pyAssetETL.s3BucketName + "/output/logs/",
        "--enable-metrics": "",
        "--enable-continuous-cloudwatch-log": "true",
        "--source_BucketName": assetBucketName,
        "--target_BucketName": assetBucketName,
        "--target_covidPrefix": covidPath,
        "--target_hiringPrefix": hiringPath,
        "--covid_source_bucket": covid_src_bucket,
        "--obj_covid_source_key": covid_src_key,
        "--obj_covid_target_key": covidPath + obj_covidCases,
        "--hiring_source_bucket": hiring_src_bucket,
        "--obj_hiring_source_key": hiring_src_key,
        "--obj_hiring_target_key": hiringPath + obj_covidHiring,
        "--obj_1_source_key": f_pyAssetETL.s3ObjectKey,
        "--obj_1_target_key": scriptsPath + obj_assets,
        "--obj_2_source_key": f_pyParquet.s3ObjectKey,
        "--obj_2_target_key": scriptsPath + obj_etl,
        "--obj_3_source_key": f_pyRedshiftLoad.s3ObjectKey,
        "--obj_3_target_key": scriptsPath + obj_redshiftLoad,
      },
      maxRetries: 2,
      timeout: 60,
      numberOfWorkers: 10,
      glueVersion: "3.0",
      workerType: "G.1X",
    });

    const glue_job_parquet = new glue.CfnJob(this, "glue-job-parquet", {
      name: "glue-workflow-parquetjob",
      description: "Convert the csv files in S3 to parquet",
      role: glue_crawler_role.roleArn,
      executionProperty: {
        maxConcurrentRuns: 1,
      },
      command: {
        name: "glueetl", //spark ETL job must be set to value of 'glueetl'
        pythonVersion: "3",
        scriptLocation:
          "s3://" + f_pyParquet.s3BucketName + "/" + scriptsPath + obj_etl,
      },
      defaultArguments: {
        "--TempDir": "s3://" + assetBucketName + "/output/temp/",
        "--job-bookmark-option": "job-bookmark-disable",
        "--job-language": "python",
        "--spark-event-logs-path":
          "s3://" + assetBucketName + "/output/logs/",
        "--enable-metrics": "",
        "--enable-continuous-cloudwatch-log": "true",
        "--glue_database_name": glue_db.databaseName,
        "--glue_covid_table": covidCasesTable,
        "--glue_hiring_table": covidHiringTable,
        "--output_bucket_name": assetBucketName,
        "--output_prefix_path": parquetPath
      },
      maxRetries: 2,
      timeout: 240,
      numberOfWorkers: 10,
      glueVersion: "3.0",
      workerType: "G.1X",
    });

    //load parquet data into Redshift
    const glue_job_redshift_load = new glue.CfnJob(
      this,
      "glue-job-redshift-load",
      {
        name: "glue-workflow-redshift-load",
        description: "Use Glue to load output data into Redshift",
        role: glue_crawler_role.roleArn,
        executionProperty: {
          maxConcurrentRuns: 1,
        },
        command: {
          name: "glueetl", //spark ETL job must be set to value of 'glueetl'
          pythonVersion: "3",
          scriptLocation:
            "s3://" +
            f_pyRedshiftLoad.s3BucketName +
            "/" +
            scriptsPath +
            obj_redshiftLoad,
        },
        defaultArguments: {
          "--TempDir": redshift_temp_dir,
          "--job-bookmark-option": "job-bookmark-disable",
          "--job-language": "python",
          "--spark-event-logs-path":
            "s3://" + assetBucketName + "/output/logs/",
          "--enable-metrics": "",
          "--enable-continuous-cloudwatch-log": "true",
          "--glue_database_name": glue_db.databaseName,
          "--glue_input_file1": obj_redshiftLoad,
          "--output_bucket_name": assetBucketName,
        },
        connections: {
          connections: ["redshift-connect"],
        },
        maxRetries: 2,
        timeout: 240,
        numberOfWorkers: 10,
        glueVersion: "3.0",
        workerType: "G.1X",
      }
    );

    //create triggers

    //rename assets and copy them to scripts folder
    const glue_trigger_assetJob = new glue.CfnTrigger(
      this,
      "glue-trigger-assetJob",
      {
        name: "Run-Job-" + glue_job_asset.name,
        workflowName: glue_workflow.name,
        actions: [
          {
            jobName: glue_job_asset.name,
            timeout: 120,
          },
        ],
        type: "ON_DEMAND",
      }
    );
    //add trigger dependency on workflow and job
    glue_trigger_assetJob.addDependsOn(glue_job_asset);
    glue_trigger_assetJob.addDependsOn(glue_workflow);

    //crawl csv files located in S3 scripts folder
    const glue_trigger_crawlJob = new glue.CfnTrigger(
      this,
      "glue-trigger-crawlJob",
      {
        name: "Run-Crawler-" + glue_crawler_s3.name,
        workflowName: glue_workflow.name,
        actions: [
          {
            crawlerName: glue_crawler_s3.name,
          },
        ],
        predicate: {
          conditions: [
            {
              logicalOperator: "EQUALS",
              jobName: glue_job_asset.name,
              state: "SUCCEEDED",
            },
          ],
          logical: "ANY",
        },
        type: "CONDITIONAL",
        startOnCreation: true,
      }
    );

    //etl job trigger to merge data and convert to parquet for Redshift load
    const glue_trigger_parquetJob = new glue.CfnTrigger(
      this,
      "glue-trigger-parquetJob",
      {
        name: "Run-Job-" + glue_job_parquet.name,
        workflowName: glue_workflow.name,
        actions: [
          {
            jobName: glue_job_parquet.name,
          },
        ],
        predicate: {
          conditions: [
            {
              logicalOperator: "EQUALS",
              crawlerName: glue_crawler_s3.name,
              crawlState: "SUCCEEDED",
            },
          ],
          logical: "ANY",
        },
        type: "CONDITIONAL",
        startOnCreation: true,
      }
    );

    //crawl parquet files located in S3 output-data folder
    const glue_trigger_crawlJob_parquet = new glue.CfnTrigger(
      this,
      "glue-trigger-crawlJob-parquet",
      {
        name: "Run-Crawler-" + glue_crawler_s3_parquet.name,
        workflowName: glue_workflow.name,
        actions: [
          {
            crawlerName: glue_crawler_s3_parquet.name,
          },
        ],
        predicate: {
          conditions: [
            {
              logicalOperator: "EQUALS",
              jobName: glue_job_parquet.name,
              state: "SUCCEEDED",
            },
          ],
          logical: "ANY",
        },
        type: "CONDITIONAL",
        startOnCreation: true,
      }
    );

    //create Glue job trigger to load output data into Redshift
    const glue_trigger_redshiftJob = new glue.CfnTrigger(
      this,
      "glue-trigger-redshiftJob",
      {
        name: "Run-Job-" + glue_job_redshift_load.name,
        workflowName: glue_workflow.name,
        actions: [
          {
            jobName: glue_job_redshift_load.name,
          },
        ],
        predicate: {
          conditions: [
            {
              logicalOperator: "EQUALS",
              crawlerName: glue_crawler_s3_parquet.name,
              crawlState: "SUCCEEDED",
            },
          ],
          logical: "ANY",
        },
        type: "CONDITIONAL",
        startOnCreation: true,
      }
    );

    //add trigger dependency on workflow, job and crawler
    glue_trigger_crawlJob.addDependsOn(glue_job_asset);
    glue_trigger_parquetJob.addDependsOn(glue_trigger_crawlJob);
    glue_trigger_crawlJob_parquet.addDependsOn(glue_trigger_parquetJob);
    glue_trigger_redshiftJob.addDependsOn(glue_trigger_crawlJob_parquet);
  }
}

export interface RedshiftVpcStackProps extends cdk.StackProps {
  glueRoleGrantSecretRead: Role;
}
