import unittest

from schedule_data_processing.app import main


class TestCLI(unittest.TestCase):

    def test_lookup(self):
        assert main(["app.py", "lookup", "ZG2362"]) == '{"aircraft_registration": "ZGAUI", "departure_airport": "MCO", "arrival_airport": "FRA", "scheduled_departure_time": "2020-01-01 16:05:00", "scheduled_takeoff_time": "2020-01-01 16:15:00", "scheduled_landing_time": "2020-01-02 00:55:00", "scheduled_arrival_time": "2020-01-02 01:05:00", "flight_number": "ZG2362", "IATATypeDesignator": "789", "TypeName": "Boeing 787-9", "Hub": "FRA", "Haul": "LH", "total_seats": "216"}'

    def test_merge(self):
        assert main(["app.py", "merge"]) == ""
