from .inquiry_service import InquiryService
import json
import logging
from exceptions.exceptions import InvalidCommandException


class DriverService:

    _logger = logging.getLogger(__name__)

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
        self.__configure_logger()
        self.inquiry_service = InquiryService()
        self.inquiry_service.set_data()

    def __configure_logger(self):
        self._logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s(%(lineno)s) - <<%(levelname)s>> - %(message)s')
        file_handler = logging.FileHandler('log.txt')
        file_handler.setFormatter(formatter)
        self._logger.addHandler(file_handler)

    def lookup(self, flight_numbers):
        self._logger.info(f"STARTED - Lookup. Flight numbers: {flight_numbers}")
        schedule = self.inquiry_service.get_schedule()
        fleet = self.inquiry_service.get_fleet()

        try:
            fleet["aircraft_registration"] = fleet["Reg"]
            joined = schedule.merge(fleet, on="aircraft_registration")
            result = joined[joined["flight_number"].isin(flight_numbers)][self.Required_Columns]
        except KeyError as msg:
            msg = f"FAILED - Lookup. {msg} - column not found"
            self._logger.error(msg)
            return msg
        for column in self.Date_columns:
            result[column] = result[column].dt.strftime('%Y-%m-%d %H:%M:%S')
        result = result.to_dict("records")
        response = json.dumps(result)
        self._logger.info(f"SUCCESS - Lookup. Response: {response}")
        return response

    def merge(self):
        self._logger.info(f"STARTED - Merge")
        schedule = self.inquiry_service.get_schedule()
        airports = self.inquiry_service.get_airports()
        fleet = self.inquiry_service.get_fleet()

        try:
            fleet["aircraft_registration"] = fleet["Reg"]
            joined = schedule.merge(fleet, on="aircraft_registration")
            airports["departure_airport"] = airports["Airport"]
            joined = joined.merge(airports, on="departure_airport", suffixes=(None, "_departure"))
            airports["arrival_airport"] = airports["Airport"]
            joined = joined.merge(airports, on="arrival_airport", suffixes=(None, "_arrival"))
            joined.drop(columns=["departure_airport_arrival"], inplace=True)
        except KeyError as msg:
            msg = f"FAILED - Merge. {msg} - column not found"
            self._logger.error(msg)
            return msg
        joined.to_csv("output.csv")
        self._logger.info(f"SUCCESS - Merge. Output File name: output.csv")
        return "Output File name: output.csv"

    def fetch_info(self, args):
        try:
            command = args[1].lower()

            if command == "lookup":
                try:
                    flight_numbers = args[2].split(",")
                    return self.lookup(flight_numbers)
                except IndexError:
                    msg = "No flight number(s) passed."
                    self._logger.warning(msg)
                    return msg
            elif command == "merge":
                return self.merge()
            else:
                msg = f"INVALID COMMAND - {command}. Try valid commands. For ex- lookup XXXXX,XXXXX or merge"
                raise InvalidCommandException(msg)
        except InvalidCommandException as msg:
            self._logger.error(msg)
            return msg
        except IndexError:
            msg = "Command not specified"
            self._logger.error(msg)
            return msg
