import json
import logging

from exceptions.exceptions import InvalidCommandException
from utils.common_utils import get_distance_flown_in_nautical_miles
from utils.common_utils import reformat_datetime_to_string
from .inquiry_service import InquiryService


class DriverService:
    """
    A driver class to perform actions based on user specific command.
    """

    _logger = logging.getLogger(__name__)

    required_lookup_columns = [
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
        "Haul",
        "distance_nm",
    ]

    date_columns = [
        "scheduled_departure_time",
        "scheduled_takeoff_time",
        "scheduled_landing_time",
        "scheduled_arrival_time",
    ]

    def __init__(self):
        self.__configure_logger()
        self.inquiry_service = InquiryService()
        self.inquiry_service.set_data()

    def __configure_logger(self):
        """
        Configures the logger object.
        """
        self._logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s(%(lineno)s) - <<%(levelname)s>> - %(message)s"
        )
        file_handler = logging.FileHandler("log.txt")
        file_handler.setFormatter(formatter)
        self._logger.addHandler(file_handler)

    def lookup(self, flight_numbers):
        """
        Method to fetch information based on flight numbers.

        :param flight_numbers:
            Flight number(s) passed a list.
        :returns: A json response containing single record for each flight number.
        """
        self._logger.info(f"STARTED - Lookup. Flight numbers: {flight_numbers}")
        schedule = self.inquiry_service.get_schedule()
        fleet = self.inquiry_service.get_fleet()
        airports = self.inquiry_service.get_airports()

        try:
            fleet["aircraft_registration"] = fleet["Reg"]
            joined = schedule.merge(fleet, on="aircraft_registration")
            airports["departure_airport"] = airports["Airport"]
            joined = joined.merge(
                airports, on="departure_airport", suffixes=(None, "_departure")
            )
            airports["arrival_airport"] = airports["Airport"]
            joined = joined.merge(
                airports, on="arrival_airport", suffixes=(None, "_arrival")
            )
            joined.drop(columns=["departure_airport_arrival"], inplace=True)
            joined = joined[joined["flight_number"].isin(flight_numbers)]
            joined[self.date_columns] = joined[self.date_columns].apply(
                func=reformat_datetime_to_string, axis=1
            )
            joined["distance_nm"] = joined.apply(
                func=get_distance_flown_in_nautical_miles, axis=1
            )
            result = joined[self.required_lookup_columns]
        except KeyError as msg:
            msg = f"FAILED - Lookup. {msg} - column not found"
            self._logger.error(msg)
            return msg
        flight_numbers_not_found = set(flight_numbers) - set(
            result["flight_number"].values
        )
        result = result.to_dict("records")
        for invalid_flight_number in flight_numbers_not_found:
            result.append(
                {
                    "flight_number": f"{invalid_flight_number}",
                    "error": "Flight not found in schedule",
                }
            )
        response = json.dumps(result)
        self._logger.info(f"SUCCESS - Lookup. Response: {response}")
        return response

    def merge(self):
        """
        Method to generate a merge csv files.
        """
        self._logger.info(f"STARTED - Merge")
        schedule = self.inquiry_service.get_schedule()
        airports = self.inquiry_service.get_airports()
        fleet = self.inquiry_service.get_fleet()

        try:
            fleet["aircraft_registration"] = fleet["Reg"]
            joined = schedule.merge(fleet, on="aircraft_registration")
            airports["departure_airport"] = airports["Airport"]
            joined = joined.merge(
                airports, on="departure_airport", suffixes=(None, "_departure")
            )
            airports["arrival_airport"] = airports["Airport"]
            joined = joined.merge(
                airports, on="arrival_airport", suffixes=(None, "_arrival")
            )
            joined.drop(columns=["departure_airport_arrival"], inplace=True)
            joined["distance_nm"] = joined.apply(
                func=get_distance_flown_in_nautical_miles, axis=1
            )
        except KeyError as msg:
            msg = f"FAILED - Merge. {msg} - column not found"
            self._logger.error(msg)
            return msg
        joined.to_csv("output.csv")
        self._logger.info(f"SUCCESS - Merge. Output File name: output.csv")
        return "Output File name: output.csv"

    def fetch_info(self, args):
        """
        Method to fetch/generate information based on command and arguments passed.

        :param args:
            List of arguments containing command and flight numbers if required.
        """
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
        except Exception as msg:
            self._logger.error(msg)
            return msg
