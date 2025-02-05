""" 
Script: Dataflow Streaming Pipeline

Description: This Dataflow script processes messages ingested from three Pub/Sub topics
    (telemetry_battery_topic, telemetry_driving_topic, telemetry_environment_topic) and:

    1. Monitors battery levels and calculates vehicle autonomy using a formula
    based on battery percentage, speed, temperature, and other factors.

    2. Aggregates driving metrics (e.g., average speed, braking intensity)
    and identifies anomalies for further analysis.

    3. Enriches environmental data with weather conditions and detects
    extreme situations that could impact vehicle performance.

    4.Incorporates real-time image classification using Vision AI to analyze
    traffic conditions. The results are used to dynamically adjust the 
    efficiency in the autonomy formula.

    5.Combines data streams into a unified telemetry dataset and publishes
    the results to a downstream topic for further procedures (Push Notifications).

EDEM. Master Big Data & Cloud 2024/2025
Professor: Javi Briones
"""

""" Import Libraries """

# A. Apache Beam Libraries
import apache_beam as beam
from apache_beam.runners import DataflowRunner
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window
from apache_beam.metrics import Metrics

# B. Apache Beam ML Libraries
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import RunInference

# C. Python Libraries
from datetime import datetime
import argparse
import logging
import json

beam.options.pipeline_options.PipelineOptions.allow_non_parallel_instruction_output = True
DataflowRunner.__test__ = False

""" Code: Helpful functions """
def ParsePubSubMessage(message):

    """
    Decodes messages from Pub/Sub for further transformation.

    Params:
        message (bytes): The raw Pub/Sub message payload to be parsed and decoded.

    Returns:
        tuple: Returns a tuple, key/value, with the vehicle ID and the complete message for further aggregation.
        tuple (str, dict): A tuple where:
        - The first element is the vehicle ID.
        - The second element is the complete message for further aggregation.
    """

    #ToDo: Complete this section

def getTrafficImage(item, api_url):

    """
    Simulates the images captured by the various cameras equipped on the vehicle

    Params:
        item (dict): A single element from the input PCollection, representing the
            payload with the upstream data.
        api_url(str): API that returns different images simulating the environment captured by the vehicle.

    Returns:
        tuple (dict, Bytes): A tuple where:
        - The first element is the same payload as input.
        - The second element is the image in bytes for model inference.
    """

    import requests
    import io

    # API call to simulate a photo captured by the radar

    #Read image from URL

    #Append image_url to the payload

    #ToDo: Complete this section

""" Code: DoFn """

class FormatFirestoreDocument(beam.DoFn):

    def __init__(self, mode, firestore_collection):
        self.mode = mode
        self.firestore_collection = firestore_collection

    def process(self, element):

        """
        Formats each payload or processed element to write it as a document in Firestore

        Params:
            element (dict): A single element from the input PCollection,representing the
                payload with the upstream data.
            mode(str): Flag that allows distinguishing between inserting critical users and non-critical ones.

        Returns:
            -
        """

        from google.cloud import firestore

        # Firestore Client

        # Write Data into Firestore
        
        #ToDo: Complete this section

class BusinessLogicDoFn(beam.DoFn):

    @staticmethod
    def _get_battery_info(battery_data: list):

        """
        Extracts the latest battery info from the data.
        """

        if not battery_data:
            return {}
        
        latest_battery = battery_data[-1]

        return {k: v for k, v in latest_battery.items() if k != "vehicle_id"}

    @staticmethod
    def _get_environment_info(environment_data: list):

        """
        Aggregates environment data to compute averages and extract the latest entry.
        """

        if not environment_data:
            return {}
        
        avg_temperature = sum(e["temperature"] for e in environment_data) / len(environment_data)
        avg_humidity = sum(e["humidity"] for e in environment_data) / len(environment_data)
        latest_environment = environment_data[-1]

        return {
            "timestamp": latest_environment["timestamp"],
            "latitude": latest_environment["latitude"],
            "longitude": latest_environment["longitude"],
            "avg_temperature": avg_temperature,
            "avg_humidity": avg_humidity,
        }

    @staticmethod
    def _get_driving_info(event_data: list):

        """
        Aggregates driving event data to compute averages and extract the latest entry.
        """

        if not event_data:
            return {}
        
        avg_speed = sum(e["speed"] for e in event_data) / len(event_data)
        avg_braking_force = sum(e["braking_force"] for e in event_data) / len(event_data)
        avg_steering_angle = sum(e["steering_angle"] for e in event_data) / len(event_data)
        latest_event = event_data[-1]

        return {
            "timestamp": latest_event["timestamp"],
            "avg_speed": avg_speed,
            "avg_braking_force": avg_braking_force,
            "avg_steering_angle": avg_steering_angle,
        }

    def process(self, element):

        """
        DoFn class that implements the business logic: checks for vehicles with 
        less than 30% battery and no charging event within the analyzed window.
        As output,it separates both cases into different output PCollections
        for further analysis.

        Params:
            element (dict): A dictionary containing vehicle telemetry data.

        Yields:
            PCollection (TaggedOutput): 
                - "critical_battery_users": Telemetry data for vehicles with critical battery levels (< 30%) and no charging events.
                - "non_critical_battery_users": Telemetry data for vehicles with sufficient battery levels or charging events.
        """

        #ToDo: Complete this section

class CalculateAutonomyDoFn(beam.DoFn):

    @staticmethod
    def _categorize_traffic(traffic_score: float):
        """
        Categorizes traffic level and determines the autonomy factor.

        Args:
            traffic_score (float): Traffic score.

        Returns:
            Tuple[str, float]: Traffic level and corresponding autonomy factor.
        """

        if traffic_score > 1.5:
            return "High", 0.7
        elif 0.5 < traffic_score <= 1.5:
            return "Medium", 0.85
        else:
            return "Low", 1.0
        
    @staticmethod
    def _calculate_efficiency(temperature: float, humidity: float, braking_force: float) -> float:
        """
        Calculates the vehicle's efficiency based on environmental and driving conditions.

        Args:
            temperature (float): Average temperature.
            humidity (float): Average humidity.
            braking_force (float): Average braking force.

        Returns:
            float: Calculated efficiency.
        """
        # Constants
        base_efficiency = 6.5
        temp_coefficient = 0.01
        humidity_coefficient = 0.005
        brake_coefficient = 0.05
        optimal_temperature = 25
        optimal_humidity = 50

        # Efficiency adjustments
        temp_adjusted = (1 - temp_coefficient * abs(temperature - optimal_temperature))
        humidity_adjusted = (1 - humidity_coefficient * abs(humidity - optimal_humidity))
        brake_adjusted = (1 + brake_coefficient * abs(braking_force))

        return base_efficiency * temp_adjusted * humidity_adjusted * brake_adjusted

    def process(self, element):

        """
        DoFn that, based on all the collected and enriched data, determines the vehicle's
        range and identifies the nearest supercharger station based on its position.

        Params:
            element(PCollection): Payload with the upstream data.

        Yields:
            dict: A dictionary containing the enriched data, including the range, 
            the various coefficients used for its calculation, and the input data, 
            to be stored in the database.
        """

        dict, traffic_score = element

        # Determine traffic level and autonomy factor
        traffic_level, autonomy_factor = self._categorize_traffic(traffic_score)

        # Autonomy = Battery available * Efficiency * traffic coefficient
        keys_to_check = [
            ('battery_info', 'battery_level'),
            ('environment_info', 'avg_temperature'),
            ('environment_info', 'avg_humidity'),
            ('driving_info', 'avg_braking_force')
        ]

        is_valid = all(key in dict[section] for section, key in keys_to_check if section in dict)

        if is_valid:

            # Input params
            #ToDo: Complete this section

            # Calculate efficiency and autonomy
            efficiency = self._calculate_efficiency(temperature, humidity, braking_force)
            autonomy = battery_available * efficiency * autonomy_factor

            # Append data to the payload
            #ToDo: Complete this section


class CloudVisionModelHandler(ModelHandler):

    def load_model(self):
        
        """Initiate the Google Vision API client."""

        from google.cloud import vision
        
        client = vision.ImageAnnotatorClient()
        return client
    
    def run_inference(self, batch, model, inference):

        """
        A RunInference class that performs label detection using a pre-trained model (Vertex AI)

        This class processes batches of images, detects labels using the label_detection
        method, and returns the results with labels and their confidence scores.

        Params:

            A list of tuples (str, bytes), where:
                str: An identifier for the image.
                bytes: The image data in binary format.

        Yields:
            tuple (dict, Bytes): A tuple where:
                - dict: A single element from the input PCollection, representing the
                    payload with the upstream data.
                - list: Detected scores for the image (label annotations).

        """

        from google.cloud import vision
        from google.cloud.vision_v1.types import Feature


        feature = Feature()
        feature.type_ = Feature.Type.LABEL_DETECTION

        images = [vision.Image(content=image_bytes) for (item, image_bytes) in batch]
        item_list = [item for (item, image_bytes) in batch]

        image_requests = [vision.AnnotateImageRequest(image=image, features=[feature]) for image in images]
        batch_image_request = vision.BatchAnnotateImagesRequest(requests=image_requests)

        model_responses = model.batch_annotate_images(request=batch_image_request).responses

        response = model_responses[0].label_annotations
        output_dict = item_list[0]

        traffic_objects = [text for text in response]

        traffic_keywords = {"traffic", "congestion", "car", "vehicle", "pedestrian", "public transport", "urban area", "city"}

        # Calculate traffic score
        traffic_score = sum(label.score for label in traffic_objects if label.description.lower() in traffic_keywords)

        yield output_dict, traffic_score


""" Code: Dataflow Process """

def run():

    """ Input Arguments """

    parser = argparse.ArgumentParser(description=('Input arguments for the Dataflow Streaming Pipeline.'))

    parser.add_argument(
                '--project_id',
                required=True,
                help='GCP cloud project name.')
    
    parser.add_argument(
                '--battery_telemetry_subscription',
                required=True,
                help='PubSub subscription used for reading battery telemetry data.')
    
    parser.add_argument(
                '--driving_telemetry_subscription',
                required=True,
                help='PubSub subscription used for reading driving telemetry data.')
    
    parser.add_argument(
                '--environment_telemetry_subscription',
                required=True,
                help='PubSub subscription used for reading environment telemetry data.')
    
    parser.add_argument(
                '--firestore_collection',
                required=True,
                help='The Firestore collection where the telemetry data will be stored.')
    
    parser.add_argument(
                '--output_topic',
                required=True,
                help='PubSub Topic for sending push notifications.')
    
    parser.add_argument(
                '--image_api',
                required=False,
                default="https://europe-southwest1-serverless-edem.cloudfunctions.net/getTrafficImages",
                help='API that returns traffic environment images to simulate the photo captured by the car.')
    
    parser.add_argument(
                '--system_id',
                required=True,
                help='System that evaluates the telemetry data of the car.')

    args, pipeline_opts = parser.parse_known_args()

    """ Pipeline """

    # A. Pipeline Options

    options = PipelineOptions(pipeline_opts,
        save_main_session=True, streaming=True, project=args.project_id)
    
    # B. Pipeline Object

    with beam.Pipeline(argv=pipeline_opts,options=options) as p:

        """
        IMPORTANT TIP. How to reduce repetition in Beam pipelines:

        telemetry_sources = {
            "battery": args.battery_telemetry_subscription,
            "driving": args.driving_telemetry_subscription,
            "environment": args.environment_telemetry_subscription,
        }

        telemetry_data = {}

        for telemetry_type, subscription in telemetry_sources.items():
            telemetry_data[telemetry_type] = (
                p
                | f"Read {telemetry_type.capitalize()} Telemetry Data From PubSub" >> #ToDo: Complete this section
                | f"Parse JSON {telemetry_type} messages" >> #ToDo: Complete this section
                | f"Fixed Window for {telemetry_type.capitalize()} Telemetry Data" >> #ToDo: Complete this section
            )
        """

        battery_data = (
            p 
                | "Read Battery Telemetry Data From PubSub" >> #ToDo: Complete this section
                | "Parse JSON battery messages" >> #ToDo: Complete this section
                | "Fixed Window for Battery Telemetry Data" >> #ToDo: Complete this section
        )

        driving_data = (
            p 
                | "Read Driving Telemetry Data From PubSub" >> #ToDo: Complete this section
                | "Parse JSON driving messages" >> #ToDo: Complete this section
                | "Fixed Window for Driving Telemetry Data" >> #ToDo: Complete this section
        )

        environment_data = (
            p 
                | "Read Environment Telemetry Data From PubSub" >> #ToDo: Complete this section
                | "Parse JSON environment messages" >> #ToDo: Complete this section
                | "Fixed Window for Environment Telemetry Data" >> #ToDo: Complete this section
        )

        # CoGroupByKey
        grouped_data = (
            battery_data, driving_data, environment_data) | "Merge PCollections" >> #ToDo: Complete this section
        
        processed_data = (grouped_data
            | "Check battery level" >> #ToDo: Complete this section
        )


        (
            processed_data.non_critical_battery_users
                | "Write non_critical_battery_users documents" >> #ToDo: Complete this section
        )

        send_data = (
            processed_data.critical_battery_users
                | "Capture Traffic Image" >> #ToDo: Complete this section
                | "Model Inference" >> RunInference(model_handler=CloudVisionModelHandler()) 
                | "Calcular Autonomia" >> #ToDo: Complete this section
        )

        (
            send_data
                | "Encode notifications" >> #ToDo: Complete this section
                | "Write notifications to PubSub" >> #ToDo: Complete this section
        )

        (
            send_data
                | "Write critical_battery_users documents" >> #ToDo: Complete this section
        )


if __name__ == '__main__':

    # Set Logs
    logging.basicConfig(level=logging.INFO)

    # Disable logs from apache_beam.utils.subprocess_server
    logging.getLogger("apache_beam.utils.subprocess_server").setLevel(logging.ERROR)

    logging.info("The process started")

    # Run Process
    run()
