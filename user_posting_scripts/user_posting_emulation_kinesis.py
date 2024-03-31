import datetime
import json
import random
from multiprocessing import Process
from time import sleep

# import boto3
import requests
import sqlalchemy
from sqlalchemy import text

random.seed(100)

with open("config.json", "r") as config_file:
    config = json.load(config_file)


class AWSDBConnector:
    def __init__(self):
        self.HOST = config["db"]["host"]
        self.USER = config["db"]["user"]
        self.PASSWORD = config["db"]["password"]
        self.DATABASE = config["db"]["name"]
        self.PORT = config["db"]["port"]

    def create_db_connector(self):
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        )
        return engine


new_connector = AWSDBConnector()


# Custom function to serialize datetime objects
def serialize_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")


# Send data to an endpoint using a POST request.
def send_to_endpoint(
    method: str, data: dict, invoke_url: str, stream_name=None, partition_key=None
):
    payload = json.dumps(
        {"StreamName": stream_name, "Data": data, "PartitionKey": partition_key},
        default=serialize_datetime,
    )
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.request(method, invoke_url, data=payload, headers=headers)
        print(response.status_code, response.text)
    except requests.exceptions.RequestException as e:
        print(f"HTTP request error: {e}")


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)

            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)

            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)

            for row in user_selected_row:
                user_result = dict(row._mapping)

            # Store data in dict
            data = {"pin": pin_result, "geo": geo_result, "user": user_result}

            # Pull endpoint from config file
            endpoints = config["endpoints"]["streams"]

            # Store child processes in list
            processes = []
            for topic, result in data.items():
                stream_name = f"streaming_{config['user_id']}_{topic}"
                partition_key = f"{topic}_partition"
                p = Process(
                    target=send_to_endpoint,
                    args=("PUT", result, endpoints[topic], stream_name, partition_key),
                )
                p.start()
                processes.append(p)

            # Close processes
            for p in processes:
                p.join()


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print("Working")
