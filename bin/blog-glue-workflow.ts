#!/usr/bin/env node

// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { BlogGlueWorkFlowStack } from '../lib/blog-glue-workflow-stack';
import { RedshiftVpcStack } from '../lib/blog-redshift-vpc-stack';


const app = new cdk.App();

const workflow_stack = new BlogGlueWorkFlowStack(app, 'workflow-stack', {
  stackName: 'workflow-stack',
  description: 'creates the Glue workflow, Crawlers, Jobs and triggers'
});

const redshift_vpc_stack = new RedshiftVpcStack(app, 'redshift-vpc-stack', {
  glueRoleGrantSecretRead: workflow_stack.glueRole,
  stackName: 'redshift-vpc-stack',
  description: 'creates the VPC, Glue Connection, and Redshift cluster'
})


