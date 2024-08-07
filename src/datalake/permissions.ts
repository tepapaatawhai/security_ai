import * as core from 'aws-cdk-lib';
import {
  aws_iam as iam,
  //aws_lakeformation as lakeformation,
  custom_resources as cr,
  aws_lambda as lambda,
  aws_logs as logs,
} from 'aws-cdk-lib';

import * as constructs from 'constructs';
import * as path from 'path'; 

export enum Permission {
  ALL = 'ALL',
  SELECT = 'SELECT',
  INSERT = 'INSERT',
  DELETE = 'DELETE',
  ALTER = 'ALTER',
  DROP = 'DROP',
  DESCRIBE = 'DESCRIBE',
  CREATE_DATABASE = 'CREATE_DATABASE',
  CREATE_TABLE = 'CREATE_TABLE',
  DATA_LOCATION_ACCESS = 'DATA_LOCATION_ACCESS',
  CREATE_LF_TAG = 'CREATE_LF_TAG',
  ASSOCIATE = 'ASSOCIATE',
  GRANT_WITH_LF_TAG_EXPRESSION = 'GRANT_WITH_LF_TAG_EXPRESSION'
}

export interface Database {
  name: string;
  catalogId?: string | undefined 
}

export interface Table {
  name: string;
  databaseName: string;
  catalogId?: string;
}


export interface LakeFormationPermissionProps extends core.StackProps {
  role: iam.IRole,
  database?:  Database | undefined,
  table?: Table | undefined,
  permissions: Permission[],
  /**
   * @default hnb659fds
   */
  cdkQualifier?: string | undefined,
}


export class LakeFormationPermission extends constructs.Construct {

  constructor(scope: constructs.Construct, id: string, props: LakeFormationPermissionProps) {
    super(scope, id);

    let lambdaProps = {}
    if (props.database) {
      lambdaProps = {
        PrincipalArn: props.role.roleArn,
        Database: {
          Name: props.database.name,
        },
        Permissions: props.permissions
      }
    }
    if (props.table) {
      lambdaProps = {
        PrincipalArn: props.role.roleArn,
        Table: {
          Name: props.table.name,
          DatabaseName: props.table.databaseName,
          CatalogId: props.table.catalogId ?? core.Aws.ACCOUNT_ID
        },
        Permissions: props.permissions

      }
    }

    const cdkExecRole = iam.Role.fromRoleArn(this, 'cdkexecRole', `arn:aws:iam::${core.Aws.ACCOUNT_ID}:role/cdk-${props.cdkQualifier ?? 'hnb659fds'}-cfn-exec-role-${core.Aws.ACCOUNT_ID}-${core.Aws.REGION}`)

    const permissionFn = new lambda.Function(this, 'adminFn', {
      role: cdkExecRole,
      runtime: lambda.Runtime.PYTHON_3_12,
      logRetention: logs.RetentionDays.ONE_MONTH,
      handler: 'batch_grant_permissions.on_event',
      code: lambda.Code.fromAsset(path.join(__dirname, '../../../projectAssets/lambda/datalake')),
      timeout: core.Duration.seconds(300),      
    })

    core.Tags.of(permissionFn).add('cfcustomresource','True')

    
    const provider = new cr.Provider(this, 'provider', {
      onEventHandler: permissionFn,
    })
    
    new core.CustomResource(this, 'AdminProvider', {
      serviceToken: provider.serviceToken,
      resourceType: 'Custom::LakeFormationPutDataLakeSettings',
      properties: lambdaProps,
    })
       
  }
}
