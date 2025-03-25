""" 
Script: Disney+ User Interactions Data Generator.

Description: A data generator that simulates all user interactions during content
 consumption on the platform, supporting Disney+ in enhancing its recommendation system.

EDEM. Master Big Data & Cloud 2024/2025
Professor: Javi Briones
"""

""" Import Libraries """
from datetime import datetime, timedelta
import logging
import random
import boto3
import time
import json
import os

""" Variables """
USER_IDS = [i for i in range(10000, 10030)]
ACTIONS = ["Watch", "Play", "Stop"]
DISNEY_CATALOG = [
    "The Mandalorian", "Loki", "Moana", "Turning Red", "Elemental", "Encanto", "Wish",
    "The Marvels", "Black Panther: Wakanda Forever", "Avatar: The Way of Water",
    "Percy Jackson and the Olympians", "Ahsoka", "Star Wars: The Bad Batch",
    "Frozen II", "Toy Story 4", "Raya and the Last Dragon", "Big Hero 6", "Lightyear",
    "Zootopia+", "Secret Invasion"
]

""" Environment Variables """
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
REGION = os.getenv('AWS_REGION', 'eu-west3')
STREAM_NAME = os.getenv('KINESIS_STREAM_NAME', 'DisneyClickstream')

""" Code: Helpful Functions """
def generate_event():

    """
    Generates a simulated user interaction event with the Disney+ platform.

    Returns:
        dict: A dictionary representing a clickstream event.
    """

    action = random.choice(ACTIONS)
    event_type = random.choice(DISNEY_CATALOG)

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "userId": str(random.choice(USER_IDS)),
        "eventType": action,
        "itemId": event_type,
        "eventValue": None
    }


""" Code: Entry Point """

def run_streaming(delay_seconds: int = 1):

    """
    Continuously generates and sends simulated clickstream events to an Amazon Kinesis Data Stream.

    Parameters:
        delay_seconds (int): Delay between events in seconds. Defaults to 1.
    """

    # Cliente de Kinesis
    kinesis = boto3.client('kinesis', region_name=REGION)

    while True:
        event = generate_event()

        logging.info(f"Event | {json.dumps(event)}")

        try:
            response = kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(event),
                PartitionKey=event["userId"]
            )
            logging.info(f'Event sent to Kinesis | ShardId: {response["ShardId"]}')
        
        except Exception as e:
            logging.info(f'Failed to send event', e)

        time.sleep(delay_seconds)

""" Run """

if __name__ == "__main__":

    try:

        # Set Logs
        logging.getLogger().setLevel(logging.INFO)

        # Run Generator
        logging.info('Initializing the data generator.')

        run_streaming()

        logging.info('Terminating the data generator.')

    except KeyboardInterrupt:
        logging.warning('Simulation stopped by user.')