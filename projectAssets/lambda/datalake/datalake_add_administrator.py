import boto3
import json


lakeformation = boto3.client('lakeformation')

def on_event(event, context):
    print(event)
    request_type = event['RequestType']
    if request_type == 'Create': return on_create(event)
    if request_type == 'Update': return on_update(event)
    if request_type == 'Delete': return on_delete(event)
    raise Exception("Invalid request type: %s" % request_type)


def on_create(event):
    adminArn = event["ResourceProperties"]['RoleArn']
    
    # first get the current admins
    lakeformation_settings = lakeformation.get_data_lake_settings()['DataLakeSettings']
    print('LAKE')
    print(lakeformation_settings)
    
    
    
    lakeformation_settings['DataLakeAdmins'].append(
        { "DataLakePrincipalIdentifier" : adminArn }
    )
    
        
    lakeformation.put_data_lake_settings(
        DataLakeSettings = lakeformation_settings
    )




def on_delete(event):
    adminArn = event["ResourceProperties"]['AdminArn']
    
    lakeformation_settings = lakeformation.get_data_lake_settings()
    
    lakeformation_settings['DataLakeSettings']['DataLakeAdmins'].remove(
        { "DataLakePrincipalIdentifier" : adminArn }
    )
        
    lakeformation.put_data_lake_settings(
        DataLakeSettings = lakeformation_settings
    )
   

def on_update(event):
    raise Exception("This is an immuatable Resource. You must remove it, and create a new one")

