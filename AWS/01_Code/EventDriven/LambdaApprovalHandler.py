""" 
Script: Approve or Reject StepFunction task from AWS Lambda.

Description: Lambda is manually triggered by a human (or system)
 to approve or reject a paused Step Function task.

EDEM. Master Big Data & Cloud 2024/2025
Professor: Javi Briones
"""

""" Import Libraries """
import logging
import boto3
import json

# Set Logs
logging.getLogger().setLevel(logging.INFO)

""" Code: Entry point """

# Initialize the Step Functions client
sfn_client = boto3.client('stepfunctions')

def lambda_handler(event, context):

    """
    This Lambda is manually triggered to send approval or rejection to Step Functions.
    
    It calls `send_task_success()` if approved, or `send_task_failure()` if rejected.
    
    Parameters:
    - event (dict): Must contain 'taskToken' and boolean 'approved'.
    - context (LambdaContext): Lambda execution metadata.

    Returns:
    - dict: Result of the approval handling.
    """

    task_token = event.get('taskToken')
    approved = event.get('approved', False)
    customer = event.get('customer')
    orderId = event.get('orderId')

    if not task_token:
        raise ValueError("Missing taskToken in input.")

    if approved:
        
        # Notify Step Functions that approval was granted
        sfn_client.send_task_success(
            taskToken=task_token,
            output=json.dumps({
                "approved": True,
                "customer": customer,
                "orderId": orderId
            })
        )
        
        return {
            "statusCode": 200,
            "body": json.dumps("Approval submitted.")
        }
    
    else:

        # Notify Step Functions that approval was rejected
        sfn_client.send_task_failure(
            taskToken=task_token,
            error="UserRejected",
            cause="The request was rejected by a human."
        )
        
        return {
            "statusCode": 200,
            "body": json.dumps("Rejection submitted.")
        }
