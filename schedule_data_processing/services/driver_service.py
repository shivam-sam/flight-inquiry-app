from .inquiry_service import InquiryService
import json


class DriverService:

    Required_Columns = [
        "aircraft_registration",
        "departure_airport",
        "arrival_airport",
        "scheduled_departure_time",
        "scheduled_takeoff_time",
        "scheduled_landing_time",
        "scheduled_arrival_time",
        "flight_number",
        "IATATypeDesignator",
        "TypeName",
        "Total",
        "Hub",
        "Haul"
    ]

    Date_columns = [
        "scheduled_departure_time",
        "scheduled_takeoff_time",
        "scheduled_landing_time",
        "scheduled_arrival_time"
    ]

    def __init__(self):
        self.inquiry_service = InquiryService()
        self.inquiry_service.set_data()

    def lookup(self, flight_numbers):
        schedule = self.inquiry_service.get_schedule()
        fleet = self.inquiry_service.get_fleet()

        fleet["aircraft_registration"] = fleet["Reg"]
        joined = schedule.merge(fleet, on="aircraft_registration")
        result = joined[joined["flight_number"].isin(flight_numbers)][self.Required_Columns]
        for column in self.Date_columns:
            result[column] = result[column].dt.strftime('%Y-%m-%d %H:%M:%S')
        result = result.to_dict("records")
        return json.dumps(result)

    def merge(self):
        schedule = self.inquiry_service.get_schedule()
        airports = self.inquiry_service.get_airports()
        fleet = self.inquiry_service.get_fleet()

        fleet["aircraft_registration"] = fleet["Reg"]
        joined = schedule.merge(fleet, on="aircraft_registration")
        airports["departure_airport"] = airports["Airport"]
        joined = joined.merge(airports, on="departure_airport", suffixes=(None, "_departure"))
        airports["arrival_airport"] = airports["Airport"]
        joined = joined.merge(airports, on="arrival_airport", suffixes=(None, "_arrival"))
        joined.drop(columns=["departure_airport_arrival"], inplace=True)
        joined.to_csv("output.csv")
        return ""

    def fetch_info(self, args):
        command = args[1]

        if command == "lookup":
            flight_numbers = args[2].split(",")
            return self.lookup(flight_numbers)

        if command == "merge":
            return self.merge()
