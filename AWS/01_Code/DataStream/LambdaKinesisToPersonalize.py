import boto3
import json
import logging
import base64
import time
from datetime import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
personalize_events_client = boto3.client('personalize-events')

# Personalize Tracking ID
PERSONALIZE_TRACKING_ID = 'YOUR_PERSONALIZE_TRACKING_ID'
USER_ID_DEFAULT = 'anonymous'

def lambda_handler(event, context):
    """
    AWS Lambda handler to process Kinesis records and send events to Amazon Personalize.

    Args:
        event (dict): Event payload from Kinesis.
        context (object): Lambda context runtime methods and attributes.
    """
    logger.info("Lambda triggered with Kinesis event")

    for record in event['Records']:
        try:
            # Decode base64 payload
            payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            data = json.loads(payload)
            logger.info(f"Decoded record: {data}")

            user_id = data.get('userId', USER_ID_DEFAULT)
            item_id = data.get('itemId')
            event_type = data.get('eventType')
            event_value = data.get('event_Value', None)
            timestamp = data.get('timestamp', int(time.time()))

            if not item_id or not event_type:
                logger.warning("Record missing itemId or eventType, skipping.")
                continue

            send_event_to_personalize(
                tracking_id=PERSONALIZE_TRACKING_ID,
                user_id=user_id,
                item_id=item_id,
                event_type=event_type,
                event_value=event_value,
                timestamp=timestamp
            )

        except Exception as e:
            logger.error(f"Error processing record: {e}", exc_info=True)

def send_event_to_personalize(tracking_id, user_id, item_id, event_type, event_value=None, timestamp=None):
    """
    Send an event to Amazon Personalize using the PutEvents API.

    Args:
        tracking_id (str): Amazon Personalize tracking ID.
        user_id (str): ID of the user who triggered the event.
        item_id (str): ID of the item involved in the event.
        event_type (str): Type of the event (e.g. 'click', 'purchase').
        event_value (float, optional): Numeric value associated with the event.
        timestamp (int, optional): Unix timestamp of the event.
    """
    if timestamp is None:
        timestamp = int(time.time())

    event = {
        'eventType': event_type,
        'sentAt': datetime.utcnow().isoformat(),
        'properties': json.dumps({
            'itemId': item_id,
            'eventValue': event_value
        })
    }

    personalize_events_client.put_events(
        trackingId=tracking_id,
        userId=user_id,
        sessionId=user_id, 
        eventList=[event]
    )

    logger.info(f"Event pushed to Personalize for user {user_id}, item {item_id}")
