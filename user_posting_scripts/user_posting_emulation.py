import datetime
import json
import random
from threading import Thread
from time import sleep

import requests
import sqlalchemy
from sqlalchemy import text

random.seed(100)

with open("config.json", "r") as config_file:
    config = json.load(config_file)


class AWSDBConnector:
    def __init__(self):
        """Initializes the AWSDBConnector with database configuration settings."""
        self.HOST = config["db"]["host"]
        self.USER = config["db"]["user"]
        self.PASSWORD = config["db"]["password"]
        self.DATABASE = config["db"]["name"]
        self.PORT = config["db"]["port"]

    def create_db_connector(self):
        """Creates and returns a SQLAlchemy engine for the database connection."""
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        )
        return engine

    def retrieve_result_from_table(self, topic_data: str, row_number: int, connection):
        """Retrieves a specific row from a given table in the database.

        Args:
            topic_data (str): Name of the table from which data is to be retrieved.
            row_number (int): The index of the row to retrieve.
            connection: The database connection object.

        Returns:
            dict: The data of the retrieved row as a dictionary.
        """
        topic_string = text(f"SELECT * FROM {topic_data} LIMIT {row_number}, 1")
        selected_row = connection.execute(topic_string)
        for row in selected_row:
            result = dict(row._mapping)
        return result

    def get_results_from_rds(self):
        """Fetches a random row from each of three specific tables in the database.

        Returns:
            dict: A dictionary containing data from the 'pinterest_data', 'geolocation_data', and 'user_data' tables.
        """
        random_row = random.randint(0, 11000)
        engine = self.create_db_connector()
        with engine.connect() as connection:
            pin_result = self.retrieve_result_from_table(
                "pinterest_data", random_row, connection
            )
            geo_result = self.retrieve_result_from_table(
                "geolocation_data", random_row, connection
            )
            user_result = self.retrieve_result_from_table(
                "user_data", random_row, connection
            )

        data = {"pin": pin_result, "geo": geo_result, "user": user_result}
        return data


def serialize_datetime(obj):
    """Custom JSON serializer for datetime objects.

    Returns:
        str: ISO formatted datetime string if object is a datetime.
    """
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")


def send_to_endpoint(
    method: str,
    data: dict,
    topic: str,
    stream_service=None,
    stream_name=None,
    partition_key=None,
):
    """Sends data to a specified endpoint using an HTTP request.

    Args:
        method (str): HTTP method to use ('POST', 'PUT').
        data (dict): Data to send.
        topic (str): Topic or table name associated with the data.
        stream_service (str): Type of streaming service ('kafka', 'kinesis').
        stream_name (str, optional): Name of the stream, required for Kinesis.
        partition_key (str, optional): Partition key, required for Kinesis.
    """
    if stream_service == "kinesis":
        payload = json.dumps(
            {"StreamName": stream_name, "Data": data, "PartitionKey": partition_key},
            default=serialize_datetime,
        )
        headers = {"Content-Type": "application/json"}
        endpoints = config["endpoints"]["streams"]

    elif stream_service == "kafka":
        payload = json.dumps({"records": [{"value": data}]}, default=serialize_datetime)
        headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}
        endpoints = config["endpoints"]["rest"]
    else:
        print("stream_service invalid")
        return

    try:
        response = requests.request(
            method, endpoints[topic], data=payload, headers=headers
        )
        print(f"Sent to {stream_service}: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"HTTP request error: {e}")


def post_data_to_api(stream_service):
    """Posts data fetched from AWS RDS to an API endpoint, based on the specified stream service.

    Args:
        stream_service (str): Specifies the stream service ('kafka' or 'kinesis') to use for posting data.
    """
    data = new_connector.get_results_from_rds()
    processes = []
    for topic, result in data.items():
        if stream_service == "kafka":
            args = ("POST", result, topic, stream_service)

        elif stream_service == "kinesis":
            stream_name = f"streaming_{config['user_id']}_{topic}"
            partition_key = f"{topic}_partition"
            args = ("PUT", result, topic, stream_service, stream_name, partition_key)
        else:
            print("Invalid 'stream_service'. Exiting")
            break

        t = Thread(target=send_to_endpoint, args=args)
        t.start()
        processes.append(t)

    for t in processes:
        t.join()


def infinite_loop_runner(func):
    """Decorator to run a function infinitely with a random sleep interval."""

    def inner(*args):
        while True:
            sleep(random.randrange(0, 2))
            func(*args)

    return inner


@infinite_loop_runner
def run_infinite_post_data_loop(stream_service):
    """Function to post data to an API in an infinite loop."""
    post_data_to_api(stream_service)


def start_data_stream():
    while True:
        stream_service = input(
            "Would you like to send data to 'Kafka' or 'Kinesis': "
        ).lower()
        if stream_service not in ("kafka", "kinesis"):
            print("Invalid input. Try again.")
            continue

        run_infinite_post_data_loop(stream_service)


if __name__ == "__main__":
    new_connector = AWSDBConnector()
    start_data_stream()
