import {
    aws_s3 as s3,
    aws_lambda as lambda,
    aws_iam as iam,
    aws_stepfunctions_tasks as step_functions_tasks,
    aws_stepfunctions as step_functions,
  } from
    'aws-cdk-lib';

import * as core from 'aws-cdk-lib'    
import * as constructs from 'constructs';
import * as path from 'path';
import * as LakeFormationPermissions  from '../datalake/permissions';
  
  
export interface IpCheckTrainingProps {
  name: string
  database: string,
  table: string,
  catalogId?: string,
}    


export class IpCheckTraining extends constructs.Construct {

  constructor(scope: constructs.Construct, id: string, props: IpCheckTrainingProps) {
    super(scope, id);

    const trainingBucket = new s3.Bucket(this, `trainingBucket-${props.name}`, {});

    const layer = lambda.LayerVersion.fromLayerVersionArn(this, 'layer', `arn:aws:lambda:${core.Aws.REGION}:336392948345:layer:AWSSDKPandas-Python312:12`)
    
    /**
     * Start A query
     */
    const startAthenaQueryFn = new lambda.Function(this, 'startAthenaQuery', {
      runtime: lambda.Runtime.PYTHON_3_12,
      memorySize: 128,
      layers: [layer],
      timeout: core.Duration.seconds(60),
      architecture: lambda.Architecture.X86_64,
      handler: 'query.start_query',
      code: lambda.Code.fromAsset(path.join(__dirname, '../../../projectAssets/lambda/insights/ipchecktraining/')),
      environment: {
        TRAINING_BUCKET_NAME: trainingBucket.bucketName,
      },
    })

    trainingBucket.grantReadWrite(startAthenaQueryFn)

    // give the lambda access to the database
    // const dbAccess = 
    new LakeFormationPermissions.LakeFormationPermission(this, 'dbAccess', {
      role: startAthenaQueryFn.role!,
      permissions: [ LakeFormationPermissions.Permission.DESCRIBE ],
      database: {
        name: props.database
      }
    })

    //give the lambda access to the tables
    //const tableAccess = 
    new LakeFormationPermissions.LakeFormationPermission(this, 'tableAccess', {
      role: startAthenaQueryFn.role!,
      permissions: [ 
        LakeFormationPermissions.Permission.SELECT,
        LakeFormationPermissions.Permission.DESCRIBE 
      ],
      table: {
        name: props.table,
        databaseName: props.database,
        catalogId: props.catalogId
      }
    })

    
  


    startAthenaQueryFn.addToRolePolicy(new iam.PolicyStatement({
      sid: 'startQuery',
      actions: [
        'iam:PassRole',
        'athena:StartQueryExecution',
        'glue:GetDatabase',
        'glue:BatchGetTable',
        'glue:GetTable',
        'lakeformation:GetDataAccess'
      ],
      effect: iam.Effect.ALLOW,
      resources: ['*']
    }));

    const startQueryTask = new step_functions_tasks.LambdaInvoke(this, "startqueryTask", {
        lambdaFunction: startAthenaQueryFn,
        outputPath: '$.Payload',
    });


    /**
     * Check the query
     */
    const checkAthenaQueryFn = new lambda.Function(this, 'checkAthenaQuery', {
      runtime: lambda.Runtime.PYTHON_3_12,
      layers: [layer],
      memorySize: 128,
      timeout: core.Duration.seconds(60),
      architecture: lambda.Architecture.X86_64,
      handler: 'query.is_query_done',
      code: lambda.Code.fromAsset(path.join(__dirname, '../../../projectAssets/lambda/insights/ipchecktraining/')),
      environment: {
        TRAINING_BUCKET_NAME: trainingBucket.bucketName,
      },
    })

    checkAthenaQueryFn.addToRolePolicy(new iam.PolicyStatement({
      sid: 'checkAthenaQuery',
      actions: ['athena:GetQueryExecution'],
      effect: iam.Effect.ALLOW,
      resources: ['*']
    }));

    const checkQueryTask = new step_functions_tasks.LambdaInvoke(this, "checkqueryTask", {
      lambdaFunction: checkAthenaQueryFn,
      outputPath: '$.Payload',
    });
      
    // a task to see if the query is ready
    const wait30s = new step_functions.Wait(
      this,
      "state-machine-wait-job", {
        time: step_functions.WaitTime.duration(core.Duration.seconds(30)),
      }
    );

    // a task to see if the query is ready
    const wait60s = new step_functions.Wait(
      this,
      "state-machine-wait60-job", {
        time: step_functions.WaitTime.duration(core.Duration.seconds(60)),
      }
    );

    // a failed job.
    const failedJob = new step_functions.Fail(this, 'Job Failed', {
      cause: 'AWS Batch Job Failed',
      error: 'DescribeJob returned FAILED',
    });

    const trainingRole = new iam.Role(this, 'trainingRole', {
      assumedBy: new iam.ServicePrincipal('sagemaker.amazonaws.com')
    })

    trainingRole.addManagedPolicy(iam.ManagedPolicy.fromManagedPolicyArn(this, 'trainingRolePolicy', 'arn:aws:iam::aws:policy/AmazonSageMakerFullAccess'))
    trainingBucket.grantReadWrite(trainingRole)

    const trainModelFn = new lambda.Function(this, 'trainModel', {
      runtime: lambda.Runtime.PYTHON_3_12,
      layers: [layer],
      memorySize: 128,
      timeout: core.Duration.seconds(60),
      architecture: lambda.Architecture.X86_64,
      handler: 'query.train_model',
      code: lambda.Code.fromAsset(path.join(__dirname, '../../../projectAssets/lambda/insights/ipchecktraining/')),
      environment: {
        TRAINING_BUCKET_NAME: trainingBucket.bucketName,
        TRAINING_ROLE: trainingRole.roleArn
      },
    })    

    trainModelFn.addToRolePolicy(new iam.PolicyStatement({
      sid: 'startTrainingModel',
      actions: [
        'iam:PassRole',
        'sagemaker:CreateTrainingJob',
        'athena:GetQueryExecution',
        'athena:GetQueryResults',
        'sagemaker:DescribeAlgorithm',
      ],
      effect: iam.Effect.ALLOW,
      resources: ['*']
    }));

    trainingBucket.grantReadWrite(trainModelFn.role!)

    const trainModel = new step_functions_tasks.LambdaInvoke(this, 'StartThetraining model', {
      lambdaFunction: trainModelFn,
      outputPath: '$.Payload',
    });


    const checkTrainingFn = new lambda.Function(this, 'checktrainModel', {
      runtime: lambda.Runtime.PYTHON_3_12,
      layers: [layer],
      memorySize: 128,
      timeout: core.Duration.seconds(60),
      architecture: lambda.Architecture.X86_64,
      handler: 'query.check_training',
      code: lambda.Code.fromAsset(path.join(__dirname, '../../../projectAssets/lambda/insights/ipchecktraining/')),
      environment: {
        TRAINING_BUCKET_NAME: trainingBucket.bucketName,
      },
    })

    checkTrainingFn.addToRolePolicy(new iam.PolicyStatement({
      sid: 'startTrainingModel',
      actions: ['sagemaker:DescribeTrainingJob'],
      effect: iam.Effect.ALLOW,
      resources: ['*']
    }));


    const check_training = new step_functions_tasks.LambdaInvoke(this, 'CheckThetraining model', {
      lambdaFunction: checkTrainingFn,
      outputPath: '$.Payload',
    });



    new step_functions.StateMachine(this, "state-machine", {
      definitionBody: step_functions.DefinitionBody.fromChainable(
        startQueryTask
        .next(wait30s)
        .next(checkQueryTask)
        .next(new step_functions.Choice(this, 'CheckQueryStatus')
          //look at the status field
          .when(step_functions.Condition.stringEquals('$.State', 'FAILED'), failedJob)
          .when(step_functions.Condition.stringEquals('$.State', 'RUNNING'), wait30s)
          .otherwise(
            trainModel
            .next(wait60s)
            .next(check_training)
            .next(new step_functions.Choice(this, 'CheckTrainingStatus')
              .when(step_functions.Condition.stringEquals('$.State', 'FAILED'), failedJob)
              .when(step_functions.Condition.stringEquals('$.State', 'RUNNING'), wait60s)
              .otherwise(new step_functions.Succeed(this, 'Success'))
            )
          )
        )
      ),
      timeout: core.Duration.minutes(60),
      stateMachineName: "TrainModel",
    });




  }
}