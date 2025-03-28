""" 
Script: AWS Lambda triggered by Step Functions using waitForTaskToken

Description: It captures the task token and input, and holds the workflow until
    manual approval is submitted.

EDEM. Master Big Data & Cloud 2024/2025
Professor: Javi Briones
"""

""" Import Libraries """
import logging
import json

# Set Logs
logging.getLogger().setLevel(logging.INFO)

""" Code: Entry point """

def lambda_handler(event, context):
    """
    Step Function invokes this Lambda with a task token.
    This function is responsible for capturing the token 
    and exposing it to a human for manual approval.
    
    Parameters:
    - event (dict): Includes taskToken and any input from the state machine.
    - context (LambdaContext): Lambda execution metadata.

    Returns:
    - dict: Confirmation that the token has been captured.
    """
    task_token = event.get("taskToken")
    user_input = event.get("input")

    logging.info("Waiting for human approval...")
    logging.info(f"Task token: {task_token}")
    logging.info(f"Input: {user_input}")

    # Return Token
    return {
        "status": "waiting_for_approval",
        "taskToken": task_token,
        "customer": user_input['detail']['customer'],
        "orderId": user_input['detail']['orderId']
    }
