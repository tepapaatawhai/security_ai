import {
  aws_s3 as s3,
  aws_sqs as sqs,
  aws_lambda as lambda,
  aws_iam as iam,
  aws_lambda_event_sources as event_sources,
  aws_ecr_assets as aws_ecr_assets,
} from 'aws-cdk-lib';

import * as core from 'aws-cdk-lib'    
import * as constructs from 'constructs';
import * as path from 'path';
  
  
    
  export interface AiBotProps {
     botName: string;
  }
    
    
export class AiBot extends constructs.Construct {
  
  constructor(scope: constructs.Construct, id: string, props: AiBotProps) {
    super(scope, id);
    
    /**
     * Create dockerImage for the Streamlit app, and publish it to ECR 
     */
    const imageAsset =  new aws_ecr_assets.DockerImageAsset(this, 'StreamLitImage', {
      directory: path.join(__dirname, '../../projectAssets/images/fred')
    });

    /**
     * Create a role for the ECS Task to use to execute the Applicaiton
     */
    const appExecRole = new iam.Role(this, 'AppExecRole', {
      assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com')
    });

    /**
     * Give it permissions to do the basic things a ecs-task needs to do. 
     */
    appExecRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        "ecr:GetAuthorizationToken",
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      effect: iam.Effect.ALLOW,
      resources: ['*']
    }));

     /**
     * This provide additional policy for execution Role to call other resoruces as required. 
     */
     appExecRole.addToPolicy(new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          // Add Bedrock permissions here
          "bedrock:InvokeModel*", 
          "bedrock:Converse*",
          "athena:GetQueryResults",
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "s3:GetBucketLocation",
          "glue:GetTable",
          "glue:GetTables",
          "glue:BatchGetTable",
          "glue:GetDatabase",
          "kms:*",
          "lakeformation:GetDataAccess",
          "iam:PassRole",
          "s3:*",
          "glue:GetPartitions",
        ],
        resources: [
          "*",
        ], // Adjust the resource as needed
    })),
   
  }
}