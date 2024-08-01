import boto3
import json
import os

from src.tools import Tools

# session = boto3.session.Session(profile_name='analyst', region_name='us-east-1')
# bedrock = session.client('bedrock-runtime', 'us-east-1', endpoint_url='https://bedrock-runtime.us-east-1.amazonaws.com')

bedrock = boto3.client('bedrock-runtime', 'us-east-1', endpoint_url='https://bedrock-runtime.us-east-1.amazonaws.com')

DATABASE_NAME = 'amazon_security_lake_glue_db_ap_southeast_2'
MODEL_ID = 'anthropic.claude-3-5-sonnet-20240620-v1:0'

tools = Tools()


def answer_query(message_list):

    system = f"""
    - You are not a human. Reply like a machine. 
    - The database is an instance of AWS Security Lake.
    - The database is served by AWS Athena, the tables are Glue Tables. 
    - Always include the the database name '{DATABASE_NAME}' in sql querys
    - Use the get_tables tool to get a list of tables in the database
    - the DATE_SUB function is not available in this version of SQL
    - Use the sql_db_query tool to query the database
    - Do not use backtick (`) in the SQL Query. 
    - Don't use table JOIN, unless you absolutely have to.
    - ALWAYS use the GROUP BY clause for columns you want to query.
    - Do not use colon `:` in the SQL Query. It causes this error "Error: (sqlalchemy.exc.InvalidRequestError) A value is required for bind parameter".
    - Avoid using aliases(`as` clause) in the SQL Query.
    - When querying column of `string` type, use single quotes ' in SQL Query for casting to string.
    - Limit replies to topics that are about questions about information in the database.
    - Limit replies to topics that are about questions about information in the database.
    - Replace the use of the word 'I' with 'Claude LLM'
    - All Times should be in New Zealand Standard Time
    """
       
    response = bedrock.converse(
        modelId=MODEL_ID,
        messages=message_list,
        inferenceConfig={
            "maxTokens": 2000,
            "temperature": 0
        },
        toolConfig={
            "tools": tools.tool_list
        },
        system=[
            { "text":system },
        ]
    )
    
    # Extract the output message from the response.
    return response['output']['message']
    