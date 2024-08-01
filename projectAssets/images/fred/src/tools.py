import boto3
from src.athenaQuery import SQLQuery, GetTables

class Tools():

    def __init__(self):
        self.tool_list = [
            {
                "toolSpec": {
                    "name": "sql_db_query",
                    "description": 'Input to this tool is a detailed and correct SQL query, output is a result from the database.\n    This tool gives access to a real databse.\n    If the query does not return anything or return blank results, it means the query is correct and returned 0 rows.\n    If the query is not correct, an error message will be returned.\n    If an error is returned, rewrite the query, check the query, and try again.\n    ',
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {
                                "sql": {
                                    "type": "string",
                                    "description": "An SQL Statement"
                                }
                            },
                            "required": ["x"]
                        }
                    }
                }
            },
            {
                "toolSpec": {
                    "name": "get_tables",
                    "description": 'Input to this tool is a detailed and correct SQL query, output is a list of tables in the database.\n    This tool gives access to a real databse.\n    If the query does not return anything or return blank results, it means the query is correct and returned 0 rows.\n    If the query is not correct, an error message will be returned.',
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {
                                "sql": {
                                    "type": "string",
                                    "description": "An SQL Statement"
                                }
                            },
                            "required": ["x"]
                        }
                    }
                }
            }
        ]

def tool_use(content):


    for content_block in content:


        if 'toolUse' in content_block:
            return {
                "toolUse": True,
                "info": f"{content_block['toolUse']['name']}: {content_block['toolUse']['input']['sql']}"
            }
            
    return {
        "toolUse": False
    }


def check_for_tool_use(content):

    follow_up_blocks = []

    for content_block in content:

        if 'toolUse' in content_block:
            
            
            match content_block['toolUse']['name']:
                case "sql_db_query":
                    query=SQLQuery(content_block['toolUse']['input']['sql'], content_block['toolUse']['toolUseId'])
                    follow_up_blocks.append(query.follow_up_block)

                case "get_tables":
                    query=GetTables(content_block['toolUse']['input']['sql'], content_block['toolUse']['toolUseId'])
                    follow_up_blocks.append(query.follow_up_block)
                
                case _:
                    pass
    
    return follow_up_blocks