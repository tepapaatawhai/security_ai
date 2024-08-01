import boto3
import os
import sagemaker


athena = boto3.client('athena')
sagemaker_client = boto3.client('sagemaker')

TRAINING_BUCKET_NAME = os.environ['TRAINING_BUCKET_NAME']


def start_query(event, context):

#This should work as an input 
#{"Query": "SELECT src_endpoint.instance_uid as instance_id, src_endpoint.ip as sourceip FROM amazon_security_lake_glue_db_ap_southeast_2.amazon_security_lake_table_ap_southeast_2_vpc_flow_2_0 LIMIT 30", "Path": "input"}



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
    training_job_name = f'{athena_execution_id}-training-job'
    response = athena.get_query_execution(QueryExecutionId=athena_execution_id)

    # https://docs.aws.amazon.com/sagemaker/latest/dg-ecr-paths/ecr-ap-southeast-2.html#ipinsights-ap-southeast-2
    
    ip_insights_image_uri = sagemaker.image_uris.retrieve(framework='ipinsights',region='ap-southeast-2')

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
            "trainingImage": "712309505854.dkr.ecr.ap-southeast-2.amazonaws.com/ipinsights:1",
            "trainingInputMode": "File",
            "enableSageMakerMetricsTimeSeries": false
        },
        RoleArn = 'arn:aws:iam::381491951558:role/sage',
        InputDataConfig=[
            {
                "channelName": "train",
                "dataSource": {
                    "s3DataSource": {
                        "s3DataType": "S3Prefix",
                        "s3Uri": "s3://prod-insights-ipchecktrainingtrainingbuckettest78a-0neejzmqgc5j/input/16ac5a56-92f6-46a2-a100-c319d4168d89.csv",
                        "s3DataDistributionType": "FullyReplicated"
                    }
                },
                "contentType": "",
                "compressionType": "None",
                "recordWrapperType": "None",
                "enableFFM": false
            }
        ],
        OutputDataConfig={ 
            "kmsKeyId": "",
            "s3OutputPath": "s3://prod-insights-ipchecktrainingtrainingbuckettest78a-0neejzmqgc5j/output/16ac5a56-92f6-46a2-a100-c319d4168d89.csv",
            "removeJobNameFromS3OutputPath": false,
            "disableModelUpload": false
        },
        R
                "resourceConfig": {
            "instanceType": "ml.m4.xlarge",
            "instanceCount": 1,
            "volumeSizeInGB": 1,
            "volumeKmsKeyId": "",
            "keepAlivePeriodInSeconds": 0
        },
        



    },
    )

    ip_insights = sagemaker.estimator.Estimator(
        image_uri = ip_insights_image_uri,
        role = sagemaker.get_execution_role(),
        instance_count=1,
        instance_type="ml.m5.large",
        output_path=f"s3://{TRAINING_BUCKET_NAME}/output",
        sagemaker_session=sagemaker.Session()
    )
    
    ip_insights.set_hyperparameters(
        num_entity_vectors="20000",
        random_negative_sampling_rate="5",
        vector_dim="128",
        mini_batch_size="1000",
        epochs="5",learning_rate="0.01"
    )

    ip_insights.fit(
        inputs= response['QueryExecution']['ResultConfiguration']['OutputLocation'],
        job_name = training_job_name
    )

    return {
        "TrainingJobName": training_job_name
    }

def check_training(event,context):
    training_job_name = event['TrainingJobName']
    response = sm_client.describe_training_job(TrainingJobName=training_job_name)
    status = response['TrainingJobStatus']
    if status == 'Completed':
        return {
            "State": "Completed",
            "TrainingJobName": training_job_name
        }
    elif status == 'Failed':
        return {
            "State": "Failed",
            "TrainingJobName": training_job_name
        }
    else:
        return {
            "State": "Running",
            "TrainingJobName": training_job_name
        }

        

def get_bucket_name(s3_uri):
    cleaned_uri = s3_uri.replace('s3://', '')
    parts = cleaned_uri.split('/', 1)
    bucket_name = parts[0]
    return bucket_name

        "resourceConfig": {
            "instanceType": "ml.m4.xlarge",
            "instanceCount": 1,
            "volumeSizeInGB": 1,
            "volumeKmsKeyId": "",
            "keepAlivePeriodInSeconds": 0
        },
        "stoppingCondition": {
            "maxRuntimeInSeconds": 86400
        },
        "tags": [],
        "enableNetworkIsolation": false,
        "enableInterContainerTrafficEncryption": false,
        "enableManagedSpotTraining": false,
        "disableEFA": false,
        "trainingJobArn": "arn:aws:sagemaker:ap-southeast-2:381491951558:training-job/sagetest-copy-07-31",
        "withWarmPoolValidationError": false
    },
    "responseElements": {
        "trainingJobArn": "arn:aws:sagemaker:ap-southeast-2:381491951558:training-job/sagetest-copy-07-31"
    },
    "requestID": "774b8bc8-646a-4717-a5fc-4b6b3cc26aa9",
    "eventID": "94991935-048d-4e0b-b514-2831bb84e4f4",
    "readOnly": false,
    "eventType": "AwsApiCall",
    "managementEvent": true,
    "recipientAccountId": "381491951558",
    "eventCategory": "Management",
    "tlsDetails": {
        "tlsVersion": "TLSv1.3",
        "cipherSuite": "TLS_AES_128_GCM_SHA256",
        "clientProvidedHostHeader": "api.sagemaker.ap-southeast-2.amazonaws.com"
    },
    "sessionCredentialFromConsole": "true"
}