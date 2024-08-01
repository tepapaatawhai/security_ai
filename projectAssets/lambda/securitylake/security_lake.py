import boto3
import json

security_lake = boto3.client('securitylake')
lakeformation = boto3.client('lakeformation')

def on_event(event, context):
    print(event)
    request_type = event['RequestType']
    if request_type == 'Create': return on_create(event)
    if request_type == 'Update': return on_update(event)
    if request_type == 'Delete': return on_delete(event)
    raise Exception("Invalid request type: %s" % request_type)

def on_create(event):
    props = event["ResourceProperties"]
    response = security_lake.create_data_lake(
        configurations=[
            {
                'encryptionConfiguration': props["encryptionConfiguration"],
                'lifecycleConfiguration':  json.loads(props["lifecycleConfiguration"]),
                'region': props["region"],
            },
        ],
        metaStoreManagerRoleArn = props["metaStoreManagerRoleArn"]
    )
    physical_id = next(item for item in response["dataLakes"] if item["region"] == props["region"])["dataLakeArn"]
    #physical_id = response["dataLakes"][0]["dataLakeArn"]

    return { 
        'PhysicalResourceId': physical_id, 
        'Data': {
            'Arn': physical_id,
            #'BucketArn': response["dataLakes"][0]["s3BucketArn"]
        }
    }

def on_delete(event):
    props = event["ResourceProperties"]
    response = security_lake.delete_data_lake(
        regions=[
            props["region"],
        ]
    )

def on_update(event):
    physical_id = event["PhysicalResourceId"]
    props = event["ResourceProperties"]
    response =security_lake.update_data_lake(
        configurations=[
            {
                'encryptionConfiguration': props["encryptionConfiguration"],
                'lifecycleConfiguration': json.loads(props["lifecycleConfiguration"]),
                'region': props["region"],
            },
        ],
        metaStoreManagerRoleArn = props["metaStoreManagerRoleArn"]
    )

def is_complete(event, context):
    physical_id = event["PhysicalResourceId"]
    request_type = event["RequestType"]
    props = event["ResourceProperties"]

    # check if resource is stable based on request_type
    response = security_lake.list_data_lakes(
        regions=[
            props["region"],
        ]
    )

    is_ready = False

    if request_type == 'Create':
        if next(item for item in response["dataLakes"] if item["region"] == props["region"])["createStatus"] == 'COMPLETED':
            is_ready = True
            # lakeformation.batch_grant_permissions(
            #     CatalogId = props["account"],
            #     Entries=[
            #         {
            #             'Id': '1',
            #             'Principal': {
            #                 'DataLakePrincipalIdentifier': props["cdkRoleArn"]
            #             },
            #             'Resource': {
            #                 'Database': {
            #                     'CatalogId': props["account"],
            #                     'Name': props["databaseName"]
            #                 }
            #             }
            #             'Permissions': [
            #                 'ALL'
            #             ],
            #             'PermissionsWithGrantOption': [
            #                 'ALL',
            #             ]
            #         }
            #     ]
            # )

            


        if next(item for item in response["dataLakes"] if item["region"] == props["region"])["createStatus"] == 'FAILED':
            print(json.dumps(response, indent=4))
            raise TypeError("DataLake Creation Failed")

    if request_type in ['Delete', 'Update']:
        is_ready = True


    
    return { 'IsComplete': is_ready }