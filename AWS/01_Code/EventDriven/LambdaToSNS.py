""" 
Script: AWS Lambda to Amazon SNS

Description: AWS Lambda function invoked by Step Functions that sends a message
 to Amazon SNS, which will then deliver the email or message to the device.

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

# Initialize the SNS client
sns_client = boto3.client('sns')

""" Code: Entry point """

def lambda_handler(event, context):

    """
    Publishes a message to an Amazon SNS topic invoked by AWS Step Functions.

    Parameters:
        event (dict): The input event triggering the Lambda function.
        context (LambdaContext): Provides runtime information about the function invocation.

    Returns:
        dict: A response object with HTTP statusCode and body indicating the result.
    """

    # Get the SNS topic ARN from environment variables
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN")

    # Message payload to be published
    msg = f"Hey {event['customer']}, Your Taco Bell Order #{event['orderId']} is on the Way!"

    # Publish the message to the SNS topic
    try:
        response = sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=json.dumps(msg),
            Subject='TacoBell'
        )

        return {
            'statusCode': 200,
            'body': json.dumps('Message successfully published to SNS.')
        }

    except Exception as err:

        return {
            'statusCode': 500,
            'body': json.dumps(f'Failed to publish message: {str(err)}')
        }