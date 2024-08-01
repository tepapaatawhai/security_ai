# IMPORTS
import os
import io
import boto3
import json
import csv
import logging
import urllib
import awswrangler as wr
import pandas as pd
from io import StringIO

# ENVIRONMENT variables
ENDPOINT_NAME = os.environ['ENDPOINT_NAME']

# LOGGER
logger = logging.getLogger()

# logger.handler == console
#csh = logging.StreamHandler()
#logger.addHandler(csh)

# logger.level
LOG_LEVEL = logging.DEBUG
logger.setLevel(LOG_LEVEL)

# LAMBDA function
def lambda_handler(event, context):
    sagemaker= boto3.client('runtime.sagemaker')
    
    # LOG event
    log_message("parsing security lake event", logging.INFO)   
    log_message(event) 
    eventRecords = event["Records"]
    
    for eventRecord in eventRecords:    
        try:
            # parse SQS message
            log_message("parsing security lake event record")
            body = eventRecord.get("body", None)
            
            # inner_records value may be None if the message is from S3 test message.
            if (body == None or type(body) != dict):
                log_message("missing event body", logging.WARN)
                continue

            # log
            log_message("processing security lake event body")

            #for index, each_inner_record in enumerate(inner_records):
            detailRecord = body.get("detail", None)
            bodyRecords = body.get("Records", None)

            if (detailRecord == None and bodyRecords == None):
                log_message("Missing event body records", logging.WARN)
                continue

            if (detailRecord != None and type(detailRecord) == dict):
                log_message("processing event body detail")
                df = read_s3_file_to_dataframe(detailRecord)
                invoke_sagemaker(sagemaker, df)

            if (bodyRecords != None and len(bodyRecords) > 0):
                log_message("processing event body s3 records")
                for bodyRecord in bodyRecords:
                    s3BodyRecord = bodyRecord.get("s3", None)
                    df = read_s3_file_to_dataframe(s3BodyRecord)
                    invoke_sagemaker(sagemaker, df)
            
        except Exception as e:
            log_message("Unexpected error occurred while getting object " + str(e), logging.ERROR)
            raise e 

# read Parquet file from S3 into in-memory dataframe
def read_s3_file_to_dataframe(s3EventRecord):
    object_key = s3EventRecord.get("object", {}).get("key", None)
    bucket_name = s3EventRecord.get("bucket", {}).get("name", None)
    s3_url = urllib.parse.unquote_plus("s3://"+bucket_name+"/"+object_key)
    log_message("reading security lake event from S3")
    # read S3 file into Pandas dataframe
    df = wr.s3.read_parquet(s3_url)

    log_message("transforming security lake event dataframe")
    # transform dataframe
    df['instance_id'] = pd.Series()
    for i in range(len(df['src_endpoint'])):
        df['instance_id'][i] = df['src_endpoint'][i]['instance_uid']
    df['sourceip'] = pd.Series()
    for i in range(len(df['src_endpoint'])):
        df['sourceip'][i] = df['src_endpoint'][i]['ip']
    cols_to_keep = ['instance_id', 'sourceip']
    df2 = df[cols_to_keep]
    df2 = df2[df2['sourceip'] != '-']
    df2 = df2[df2['sourceip'].notnull()]
    df2 = df2[df2['instance_id']!= '-']
    df2 = df2[df2['instance_id'].notnull()]

    log_message(df)
    # return transformed dataframe
    return df2

def invoke_sagemaker(sagemaker, df):
    if(df.empty == True):
        log_message('Missing event dataframe', logging.WARN)
        return None

    # transform dataframe into temporary CSV file
    csv_file = io.StringIO()
    csvinfer = df.to_csv(csv_file, sep=',', header=False, index=False)
    inference_request = csv_file.getvalue()
    log_message(inference_request)

    # invoke SageMaker inference endpoint using CSV-formatted dataframe as request payload
    try:
        inference_response = sagemaker.invoke_endpoint(EndpointName=ENDPOINT_NAME, ContentType='text/csv', Body=inference_request)
        log_message(inference_response)
        result = json.loads(inference_response['Body'].read().decode())
        log_message(result,logging.INFO)
        return result
    except Exception as e:
        log_message("Error when invoking SageMaker endpoint: " + str(e),logging.ERROR)
        raise e


def log_message(msg, lvl=logging.DEBUG):
    if (lvl >= LOG_LEVEL):
        print(msg)