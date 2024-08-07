import * as core from 'aws-cdk-lib';
import {
  aws_iam as iam,
  aws_lambda as lambda,
  custom_resources as cr,
  aws_logs as logs,
} from 'aws-cdk-lib';
import * as constructs from 'constructs';
import * as path from 'path';


export interface DatalakeAdminProps extends core.StackProps {
  role: iam.IRole
}

export class DatalakeAdmin extends constructs.Construct {

    constructor(scope: constructs.Construct, id: string, props: DatalakeAdminProps) {
        super(scope, id);

        const adminFn = new lambda.Function(this, 'adminFn', {
          runtime: lambda.Runtime.PYTHON_3_12,
          logRetention: logs.RetentionDays.ONE_MONTH,
          handler: 'datalake_add_administrator.on_event',
          code: lambda.Code.fromAsset(path.join(__dirname, '../../../projectAssets/lambda/datalake')),
          timeout: core.Duration.seconds(300),
        })

        adminFn.addToRolePolicy(
          new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            resources: ['*'],
            actions: [
              'lakeformation:PutDataLakeSettings',
              'lakeformation:GetDataLakeSettings'
            ]
          })
        );

        const provider = new cr.Provider(this, 'provider', {
          onEventHandler: adminFn,
        })
        
        new core.CustomResource(this, 'AdminProvider', {
          serviceToken: provider.serviceToken,
          resourceType: 'Custom::LakeFormationPutDataLakeSettings',
          properties: {
            RoleArn: props.role.roleArn
          }
        })

          



    }
} 