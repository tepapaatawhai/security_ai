import boto3
import json


lakeformation = boto3.client('lakeformation')

def on_event(event, context):
    print(event)
    request_type = event['RequestType']
    if request_type == 'Create': return on_create(event)
    if request_type == 'Update': return on_create(event)
    if request_type == 'Delete': return on_delete(event)
    raise Exception("Invalid request type: %s" % request_type)


def on_create(event):

    props = event["ResourceProperties"]
    print(props)

    if 'Database' in props.keys():
        Resource = {
            'Database': {
                'Name': props['Database']['Name']
            },
        }
    
    if 'Table' in props.keys():
        Resource = {
            'Table': {
                'DatabaseName': props['Table']['DatabaseName'],
                'Name': props['Table']['Name'],
                'CatalogId': props['Table']['CatalogId']
            }
        }

    response = lakeformation.batch_grant_permissions(
        Entries=[
            {
                'Id': '1',
                'Principal': {
                    'DataLakePrincipalIdentifier': props['PrincipalArn']
                },
                'Resource': Resource,
                'Permissions': props['Permissions'],
            },
        ]
    )


def on_delete(event):

    
    props = event["ResourceProperties"]

    if 'Database' in props.keys():
        Resource = {
            'Database': {
                'Name': props['Database']['Name']
            },
        }
    
    if 'Table' in props.keys():
        Resource = {
            'DatabaseName': props['Table']['DatabaseName'],
            'Name': props['Table']['Name'],
            'CatalogId': props['Table']['CatalogId']
        }

    response = lakeformation.batch_revoke_permissions(
        Entries=[
            {
                'Id': '1',
                'Principal': {
                    'DataLakePrincipalIdentifier': props['PrincipalArn']
                },
                'Resource': Resource,
                'Permissions': props['Permissions'],
            },
        ]
    )

    

def on_update(event):
    raise Exception("This is an immuatable Resource. You must remove it, and create a new one")

