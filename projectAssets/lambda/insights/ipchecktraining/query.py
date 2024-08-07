import boto3
import os
import awswrangler as wr


athena = boto3.client('athena')
sagemaker_client = boto3.client('sagemaker')

TRAINING_BUCKET_NAME = os.environ['TRAINING_BUCKET_NAME']


def start_query(event, context):

#This should work as an input 
#{"Query": "SELECT src_endpoint.instance_uid as instance_id, src_endpoint.ip as sourceip FROM amazon_security_lake_glue_db_ap_southeast_2.amazon_security_lake_table_ap_southeast_2_vpc_flow_2_0 WHERE src_endpoint.ip IS NOT NULL AND src_endpoint.instance_uid IS NOT NULL AND src_endpoint.instance_uid != '-' AND src_endpoint.ip != '-' LIMIT 1000", "Path": "input"}

#"SELECT src_endpoint.instance_uid as instance_id, src_endpoint.ip as sourceip FROM amazon_security_lake_table_"+seclakeregion+"_vpc_flow_1_0 #WHERE src_endpoint.ip IS NOT NULL AND src_endpoint.instance_uid IS NOT NULL AND src_endpoint.instance_uid != '-' AND src_endpoint.ip != '-'"

    response = athena.start_query_execution(
        QueryString=event['Query'],
        ResultConfiguration={"OutputLocation": f"s3://{TRAINING_BUCKET_NAME}/{event['Path']}"}
    )

    return {
        "QueryExecutionId": response["QueryExecutionId"]
    }


def is_query_done(event, context):

    execution_id = event['QueryExecutionId']



    response = athena.get_query_execution(QueryExecutionId=execution_id)
    state = response["QueryExecution"]["Status"]["State"]

    if state in ['FAILED','CANCELLED']:
        return {
        "State": "FAILED",
        "QueryExecutionId": execution_id
        }

    elif state in ['QUEUED','RUNNING']:
        return {
            "State": "RUNNING",
            "QueryExecutionId": execution_id
        }

    if state == 'SUCCEEDED':
        return {
            "State": "SUCCEEDED",
            "QueryExecutionId": execution_id
        }


def train_model(event, context):

    athena_execution_id = event['QueryExecutionId']


     # Get AWS Athena SQL query results as a Pandas DataFrame.
    results = wr.athena.get_query_results(
        query_execution_id=athena_execution_id
    )

    training_path = f"s3://{TRAINING_BUCKET_NAME}/training_data/{athena_execution_id}.csv"

    # https://aws-sdk-pandas.readthedocs.io/en/stable/stubs/awswrangler.athena.get_query_results.html
    wr.s3.to_csv(
        results, 
        training_path,
        header=False,
        index=False
    )

    training_job_name = f'{athena_execution_id}-training-job'
    

    # https://docs.aws.amazon.com/sagemaker/latest/dg-ecr-paths/ecr-ap-southeast-2.html#ipinsights-ap-southeast-2
    
    
    training_request = sagemaker_client.create_training_job(
        TrainingJobName = f'{athena_execution_id}-training-job',
        HyperParameters={
            "vector_dim": "128",
            "random_negative_sampling_rate": "1",
            "weight_decay": "0.00001",
            "shuffled_negative_sampling_rate": "1",
            "num_ip_encoder_layers": "1",
            "num_entity_vectors": "20000",
            "epochs": "10",
            "learning_rate": "0.01",
            "batch_metrics_publish_interval": "1000",
            "mini_batch_size": "5000"
        },
        AlgorithmSpecification={
            "TrainingImage": "712309505854.dkr.ecr.ap-southeast-2.amazonaws.com/ipinsights:1",
            "TrainingInputMode": "File",
            "EnableSageMakerMetricsTimeSeries": False,
        },
        RoleArn = os.environ['TRAINING_ROLE'],
        InputDataConfig=[
            {
                "ChannelName": "train",
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": training_path,
                        "S3DataDistributionType": "FullyReplicated"
                    }
                },
                "ContentType": "text/csv",
                "CompressionType": "None",
                "RecordWrapperType": "None",
                #"EnableFFM": False
            }
        ],
        OutputDataConfig={ 
            "KmsKeyId": "",
            "S3OutputPath": f"s3://{TRAINING_BUCKET_NAME}/output/{athena_execution_id}.csv",
            #"RemoveJobNameFromS3OutputPath": False,
            #"DisableModelUpload": False
        },
        ResourceConfig={
            'InstanceType': 'ml.m4.xlarge',
            'InstanceCount': 1,
            'VolumeSizeInGB': 1,
            'VolumeKmsKeyId': '',
            'KeepAlivePeriodInSeconds': 0,
        },
        StoppingCondition={
            'MaxRuntimeInSeconds': 86400,
        },
        EnableNetworkIsolation=False,
        EnableInterContainerTrafficEncryption=False,
        EnableManagedSpotTraining=False,
    )

    return {
        "TrainingJobName": training_job_name
        }





def check_training(event,context):
    training_job_name = event['TrainingJobName']
    response = sagemaker_client.describe_training_job(TrainingJobName=training_job_name)
    status = response['TrainingJobStatus']
    if status == 'Completed':
        return {
            "State": "COMPLETED",
            "TrainingJobName": training_job_name
        }
    elif status == 'Failed':
        return {
            "State": "FAILED",
            "TrainingJobName": training_job_name
        }
    else:
        return {
            "State": "RUNNING",
            "TrainingJobName": training_job_name
        }

        

def get_bucket_name(s3_uri):
    cleaned_uri = s3_uri.replace('s3://', '')
    parts = cleaned_uri.split('/', 1)
    bucket_name = parts[0]
    return bucket_name