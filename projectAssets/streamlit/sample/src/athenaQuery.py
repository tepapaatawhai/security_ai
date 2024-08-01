import boto3
import time
import json
import sys

# this needs to change if running locally, swap lines 7and 9 for line 10
# session = boto3.session.Session(profile_name='analyst', region_name='ap-southeast-2')
# athena = session.client('athena')

athena = boto3.client('athena')

# this will need to be modified
RESULT_OUTPUT_LOCATION = "s3://athenaresultsdoc22/" 

class GetTables():

    def __init__(self, query, tool_use_id):
        self.query = query
        self.follow_up_block = []

        response = athena.start_query_execution(
            QueryString=query,
            ResultConfiguration={"OutputLocation": RESULT_OUTPUT_LOCATION}
        )

        has_query_succeeded(response["QueryExecutionId"])

        response = athena.get_query_results(
            QueryExecutionId=response["QueryExecutionId"]
        )

        rows = []
        for row in response['ResultSet']['Rows']:
            rows.append((row['Data'][0]['VarCharValue']))

        self.result = rows

        self.follow_up_block = {
            "toolResult": {
                "toolUseId": tool_use_id,
                "content": [
                    {
                        "json": {
                            "result": rows
                        }
                    }
                ]
            }
        }


class SQLQuery():

    def __init__(self, query, tool_use_id):
        self.query = query
        self.follow_up_block = []

        
        try:
            response = athena.start_query_execution(
                QueryString=query,
                ResultConfiguration={"OutputLocation": RESULT_OUTPUT_LOCATION}
            )
            query_success = has_query_succeeded(response["QueryExecutionId"])
        

            if query_success["state"] == 'SUCCEEDED':
        
                response = athena.get_query_results(
                    QueryExecutionId=response["QueryExecutionId"]
                )

                self.follow_up_block = {
                    "toolResult": {
                        "toolUseId": tool_use_id,
                        "content": [
                            {
                                "json": {
                                    "result": response['ResultSet']['Rows']
                                }
                            }
                        ]
                    }
                }
            
            elif query_success["state"] == 'FAILED':


                self.result = query_success["response"]["QueryExecution"]["Status"]["AthenaError"]

                self.follow_up_block = {
                    "toolResult": {
                        "toolUseId": tool_use_id,
                        "content": [
                            {
                                "json": {
                                    "result": query_success["response"]["QueryExecution"]["Status"]["AthenaError"]
                                }
                            }
                        ]
                    }
                }


            elif query_success["state"] == 'TIMEDOUT':

                sys.exit("QUERY TIMED OUT")

        except Exception as e:
            
            self.follow_up_block = {
                "toolResult": {
                    "toolUseId": tool_use_id,
                    "content": [
                        {
                            "json": {
                                "result": json.dumps(e, default=str)
                            }
                        }
                    ]
                }
            }




        

def has_query_succeeded(execution_id):
    loop_time = 1
    max_execution = 15
    state = 'RUNNING'

    while max_execution > 0 and state in ["RUNNING", "QUEUED"]:

        max_execution -= 1
        response = athena.get_query_execution(QueryExecutionId=execution_id)

        state = response["QueryExecution"]["Status"]["State"]

        if state == 'FAILED':
            return {
                "state": "FAILED",
                "response" : response 
            }

        if state == 'SUCCEEDED':
            return {
                "state": "SUCCEEDED",
                "response" : response 
            }

        
        time.sleep(loop_time)
        if loop_time == 16:
            loop_time = 32
        else:
            loop_time = loop_time * 2

    return {"state": "TIMED_OUT"}

