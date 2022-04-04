import io
import os

import pandas as pd

from azure.storage.blob import BlobClient

SCHEDULE = None
FLEET = None
AIRPORTS = None


def cleanup_temp_files(schedule_file, fleet_file, airports_file):
    for x in [schedule_file, fleet_file, airports_file]:
        if os.path.exists(x):
            os.remove(x)


def get_schedule():
    global SCHEDULE
    blob = BlobClient.from_connection_string(conn_str="DefaultEndpointsProtocol=https;AccountName=zerogrecruiting;AccountKey=q9HNK+vY0InVSBmwM45KcOL7BZJJyBMWDwTNdwKPuqS83Iq8RP4lWETgCUKQkOOsJg4WjAsgdb21Dl8JpU6vkQ==;EndpointSuffix=core.windows.net", container_name="python-case-study", blob_name="schedule.json")
    with open("schedule.json", "wb") as f:
        blob.download_blob().readinto(f)
    SCHEDULE = pd.read_json("schedule.json")
    cleanup_temp_files("schedule.json", "fleet.csv", "airports.csv")
    return SCHEDULE


def get_fleet():
    global FLEET
    blob = BlobClient.from_connection_string(conn_str="DefaultEndpointsProtocol=https;AccountName=zerogrecruiting;AccountKey=q9HNK+vY0InVSBmwM45KcOL7BZJJyBMWDwTNdwKPuqS83Iq8RP4lWETgCUKQkOOsJg4WjAsgdb21Dl8JpU6vkQ==;EndpointSuffix=core.windows.net", container_name="python-case-study", blob_name="fleet.csv")
    with open("fleet.csv", "wb") as f:
        blob.download_blob().readinto(f)
    FLEET = pd.read_csv("fleet.csv")
    cleanup_temp_files("schedule.json", "fleet.csv", "airports.csv")
    return FLEET


def get_airports():
    global AIRPORTS
    blob = BlobClient.from_connection_string(conn_str="DefaultEndpointsProtocol=https;AccountName=zerogrecruiting;AccountKey=q9HNK+vY0InVSBmwM45KcOL7BZJJyBMWDwTNdwKPuqS83Iq8RP4lWETgCUKQkOOsJg4WjAsgdb21Dl8JpU6vkQ==;EndpointSuffix=core.windows.net", container_name="python-case-study", blob_name="airports.csv")
    with open("airports.csv", "wb") as f:
        blob.download_blob().readinto(f)
    AIRPORTS = pd.read_csv("airports.csv")
    cleanup_temp_files("schedule.json", "fleet.csv", "airports.csv")
    return AIRPORTS


def get_data():
    get_schedule()
    get_airports()
    get_fleet()
