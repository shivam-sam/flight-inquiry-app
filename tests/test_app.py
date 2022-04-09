import unittest
from schedule_data_processing.app import main
import pandas as pd
from unittest import mock
import os
import json


def get_file_path(folder_name, file_name):
    this_dir_path = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(this_dir_path, "data", folder_name, file_name)
    return file_path


def test_data(file_path):
    if file_path.split(".")[-1] == "json":
        return pd.read_json(file_path)
    elif file_path.split(".")[-1] == "csv":
        return pd.read_csv(file_path)
    else:
        return pd.DataFrame()


def read_json_data(file_path):
    with open(file_path, "r") as f:
        data = json.load(f)
    return data


def fetch_blob_data(data_service_obj, container_name, blob_name):
    file_path = get_file_path("mock_blob_data", blob_name)
    return test_data(file_path)


class TestCLI(unittest.TestCase):
    @mock.patch(
        "services.data_service.DataService.fetch_data_from_blob_store", fetch_blob_data
    )
    def test_lookup_with_valid_invalid_flights(self):
        flight_numbers = "ZG2361,ZG5001,AAAAAA,CCCCC"
        response = main(["schedule_data_processing/app.py", "lookup", flight_numbers])
        actual_response = json.loads(response)

        flight_numbers = flight_numbers.split(",")
        expected_response_json_file_path = get_file_path(
            "expected_data", "lookup_expected_response_valid_invalid_flights.json"
        )
        expected_response = read_json_data(expected_response_json_file_path)

        assert len(flight_numbers) == len(actual_response)
        for row in actual_response:
            assert row["flight_number"] in flight_numbers
        assert actual_response.sort(key=lambda x: x['flight_number']) == expected_response.sort(key=lambda x: x['flight_number'])

    @mock.patch(
        "services.data_service.DataService.fetch_data_from_blob_store", fetch_blob_data
    )
    def test_lookup_with_invalid_flights(self):
        flight_numbers = "ZG2361ZG5001,AAAAAA,CCCCC"
        response = main(["schedule_data_processing/app.py", "lookup", flight_numbers])
        actual_response = json.loads(response)

        flight_numbers = flight_numbers.split(",")
        expected_response_json_file_path = get_file_path(
            "expected_data", "lookup_expected_response_invalid_flights.json"
        )
        expected_response = read_json_data(expected_response_json_file_path)

        assert len(flight_numbers) == len(actual_response)
        for row in actual_response:
            assert row["flight_number"] in flight_numbers
        assert actual_response.sort(key=lambda x: x['flight_number']) == expected_response.sort(key=lambda x: x['flight_number'])

    @mock.patch(
        "services.data_service.DataService.fetch_data_from_blob_store", fetch_blob_data
    )
    def test_merge(self):
        response = main(["schedule_data_processing/app.py", "merge"])
        assert response == "Output File name: output.csv"
