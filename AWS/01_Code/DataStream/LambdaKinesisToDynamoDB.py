""" 
Script: AWS Lambda from Kinesis to DynamoDB

Description: Lambda function that listens to events from Kinesis Data Streams
 and inserts the values into a DynamoDB table.

EDEM. Master Big Data & Cloud 2024/2025
Professor: Javi Briones
"""

""" Import Libraries """
import logging
import base64
import boto3
import json
import os

# Set Logs
logging.getLogger().setLevel(logging.INFO)

""" Environment Variables """
DYNAMO_DB_TABLE_NAME = os.getenv('DYNAMO_DB_TABLE_NAME', 'DisneyClickstreamTable')

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(DYNAMO_DB_TABLE_NAME)

""" Code: Entry point """

def lambda_handler(event, context):

    """
    Reads events from Kinesis Data Streams and inserts them into a DynamoDB table.

    Parameters:
        event (dict): The event payload that triggered the Lambda function.
            In this case, it contains records from Kinesis Data Streams.
        context (LambdaContext): Contains metadata about the invocation, function, and execution environment.

    Returns:
        dict: A response object with HTTP statusCode and body indicating the result.
    """

    for record in event['Records']:

        # Decode Kinesis Message
        payload = base64.b64decode(record['kinesis']['data'])

        try:
            data = json.loads(payload)
            logging.info(f"Event received: {data}")

            # Insert into DynamoDB
            response = table.put_item(
                Item={
                    'user_id': data['userId'],
                    'timestamp': data['timestamp'],
                    'item': data['itemId']
                }
            )

            logging.info("Successfully inserted into DynamoDB.")

        except Exception as err:
            logging.error(f"Failed to process record: {err}")

    return {
        'statusCode': 200,
        'body': 'Processed {} records.'.format(len(event['Records']))
    }
