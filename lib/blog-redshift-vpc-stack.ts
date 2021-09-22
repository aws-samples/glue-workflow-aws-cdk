// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

import * as cdk from '@aws-cdk/core'
import * as redshift from '@aws-cdk/aws-redshift';
import * as ec2 from '@aws-cdk/aws-ec2';
import * as sm from '@aws-cdk/aws-secretsmanager';
import * as glue from '@aws-cdk/aws-glue'
import { Subnet } from '@aws-cdk/aws-ec2';
import { RedshiftVpcStackProps } from './blog-glue-workflow-stack';


export class RedshiftVpcStack extends cdk.Stack {
    constructor(scope: cdk.App, id: string, props: RedshiftVpcStackProps) {
        super(scope, id, props);

        //create the vpc for Redshift
        const redshift_vpc = new ec2.Vpc(this, 'redshift-vpc', {
            cidr: '10.0.0.0/16',
            maxAzs: 1,
            subnetConfiguration: [
            {
                subnetType: ec2.SubnetType.PUBLIC,
                name: 'redshift-subnet'
            }
            ]
        });

        const redshift_subnet = redshift_vpc.selectSubnets({ 
            subnetGroupName: 'redshift-subnet'
        })

        //security group for Redshift
        const redshift_sg = new ec2.SecurityGroup(this, 'redshift-sg', {
            vpc: redshift_vpc,
            allowAllOutbound: true,
            securityGroupName: 'redshift-sg',
            description: 'redshift security group'
        });

        //add an ingress rule that self references the security group to allow Glue traffic
        redshift_sg.addIngressRule(redshift_sg, ec2.Port.tcpRange(0, 65535), 'allow glue to connect to redshift')

        //returns the subnet as a string to be used in the Glue Connection
        function getSubnetIds() {
            return redshift_vpc.selectSubnets({ subnetGroupName: 'redshift-subnet' }).subnetIds.toString()
        }

        //create s3 Endpoint for Glue Connection to use
        const s3_endpoint = new ec2.GatewayVpcEndpoint(this, 'vpc-endpoint', {
            vpc: redshift_vpc,
            service: new ec2.GatewayVpcEndpointAwsService('s3'),
            subnets: [
            redshift_subnet
            ]
        });

        //Generate secret for Redshift Cluster
        const red_secret = new sm.Secret(this, 'red-secret', {
            generateSecretString: {
            secretStringTemplate: JSON.stringify({ username: 'etladmin' }),
            generateStringKey: 'password',
            passwordLength: 16,
            excludeCharacters: '/@\"\\\'',
            excludePunctuation: true
            },
        });

        //Grant Secrets Manager read permissions to Glue role
        red_secret.grantRead(props.glueRoleGrantSecretRead);
        
        //Create Redshift cluster
        const redshift_cluster = new redshift.Cluster(this, 'redshift-covid-hiring', {
            clusterName: 'cluster-covid-hiring',
            clusterType: redshift.ClusterType.SINGLE_NODE,
            defaultDatabaseName: 'db-covid-hiring',
            nodeType: redshift.NodeType.DC2_LARGE,
            masterUser: {
            masterUsername: 'etladmin',
            masterPassword: red_secret.secretValueFromJson('password')
            },
            vpc: redshift_vpc,
            vpcSubnets: redshift_subnet,
            publiclyAccessible: false,
            securityGroups: [
            redshift_sg
            ],
            removalPolicy: cdk.RemovalPolicy.DESTROY,
        })

        //get the subnet ID for Glue Connection to use
        const glue_sub = Subnet.fromSubnetAttributes(this, 'glue-sub', {
            subnetId: getSubnetIds(),
            availabilityZone: 'us-east-1a'
        })

        //redshift jdbc url
        const redshift_jdbc_url = 'jdbc:redshift://'+redshift_cluster.clusterEndpoint.socketAddress+'/db-covid-hiring'
        
        //create Glue Connection to Redshift after Redshift cluster is available
        const red_connect = new glue.Connection(this, 'red-connect', {
            type: glue.ConnectionType.JDBC,
            connectionName: 'redshift-connect',
            securityGroups: [
            redshift_sg
            ],
            subnet: glue_sub,
            properties: {
            JDBC_ENFORCE_SSL: 'FALSE',
            JDBC_CONNECTION_URL: redshift_jdbc_url,
            USERNAME: 'etladmin',
            PASSWORD: `{{resolve:secretsmanager:${red_secret.secretName}:SecretString:password}}`
            }
        })
    }
}