import json
import logging

from exceptions.exceptions import InvalidCommandException
from utils.common_utils import get_distance_flown_in_nautical_miles
from utils.common_utils import reformat_datetime_to_string
from services.inquiry_service import InquiryService
from constants.constants import (FleetDataColumns, AirportDataColumns, ScheduleDataColumns, MergedDataColumns)
from constants.constants import AvailableInquiryCommands


class DriverService:
    """
    A driver class to perform actions based on user specific command.
    """

    _logger = logging.getLogger(__name__)

    required_lookup_columns = [
        FleetDataColumns.AIRCRAFT_REGISTRATION.value,
        AirportDataColumns.DEPARTURE_AIRPORT.value,
        AirportDataColumns.ARRIVAL_AIRPORT.value,
        ScheduleDataColumns.SCHEDULED_DEPARTURE_TIME.value,
        ScheduleDataColumns.SCHEDULED_TAKEOFF_TIME.value,
        ScheduleDataColumns.SCHEDULED_LANDING_TIME.value,
        ScheduleDataColumns.SCHEDULED_ARRIVAL_TIME.value,
        ScheduleDataColumns.FLIGHT_NUMBER.value,
        FleetDataColumns.IATA_TYPE_DESIGNATOR.value,
        FleetDataColumns.TYPE_NAME.value,
        FleetDataColumns.TOTAL.value,
        FleetDataColumns.HUB.value,
        FleetDataColumns.HAUL.value,
        MergedDataColumns.DISTANCE_IN_NM.value,
    ]

    date_columns = [
        ScheduleDataColumns.SCHEDULED_DEPARTURE_TIME.value,
        ScheduleDataColumns.SCHEDULED_TAKEOFF_TIME.value,
        ScheduleDataColumns.SCHEDULED_LANDING_TIME.value,
        ScheduleDataColumns.SCHEDULED_ARRIVAL_TIME.value,
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
            fleet[FleetDataColumns.AIRCRAFT_REGISTRATION.value] = fleet[FleetDataColumns.REG.value]
            joined = schedule.merge(fleet, on=FleetDataColumns.AIRCRAFT_REGISTRATION.value)
            airports[AirportDataColumns.DEPARTURE_AIRPORT.value] = airports[AirportDataColumns.AIRPORT.value]
            joined = joined.merge(
                airports, on=AirportDataColumns.DEPARTURE_AIRPORT.value, suffixes=(None, "_departure")
            )
            airports[AirportDataColumns.ARRIVAL_AIRPORT.value] = airports[AirportDataColumns.AIRPORT.value]
            joined = joined.merge(
                airports, on=AirportDataColumns.ARRIVAL_AIRPORT.value, suffixes=(None, "_arrival")
            )
            joined.drop(columns=["departure_airport_arrival"], inplace=True)

            joined = joined[joined[ScheduleDataColumns.FLIGHT_NUMBER.value].isin(flight_numbers)]
            if joined.size == 0:
                result = self.handle_invalid_flight(flight_numbers)
                response = json.dumps(result)
                self._logger.warning(f"CAUTION - Lookup. All flights invalid. Response: {response}")
                return response

            joined[self.date_columns] = joined[self.date_columns].apply(
                func=reformat_datetime_to_string, axis=1
            )
            joined[MergedDataColumns.DISTANCE_IN_NM.value] = joined.apply(
                func=get_distance_flown_in_nautical_miles, axis=1
            )
            result = joined[self.required_lookup_columns]
        except KeyError as msg:
            msg = f"FAILED - Lookup. {msg} - column not found"
            self._logger.error(msg)
            return msg
        flight_numbers_not_found = set(flight_numbers) - set(
            result[ScheduleDataColumns.FLIGHT_NUMBER.value].values
        )
        result = result.to_dict("records")
        result.extend(self.handle_invalid_flight(flight_numbers_not_found))
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
            fleet[FleetDataColumns.AIRCRAFT_REGISTRATION.value] = fleet[FleetDataColumns.REG.value]
            joined = schedule.merge(fleet, on=FleetDataColumns.AIRCRAFT_REGISTRATION.value)
            airports[AirportDataColumns.DEPARTURE_AIRPORT.value] = airports[AirportDataColumns.AIRPORT.value]
            joined = joined.merge(
                airports, on=AirportDataColumns.DEPARTURE_AIRPORT.value, suffixes=(None, "_departure")
            )
            airports[AirportDataColumns.ARRIVAL_AIRPORT.value] = airports[AirportDataColumns.AIRPORT.value]
            joined = joined.merge(
                airports, on=AirportDataColumns.ARRIVAL_AIRPORT.value, suffixes=(None, "_arrival")
            )
            joined.drop(columns=["departure_airport_arrival"], inplace=True)
            joined[MergedDataColumns.DISTANCE_IN_NM.value] = joined.apply(
                func=get_distance_flown_in_nautical_miles, axis=1
            )
        except KeyError as msg:
            msg = f"FAILED - Merge. {msg} - column not found"
            self._logger.error(msg)
            return msg
        joined.to_csv("output.csv")
        self._logger.info(f"SUCCESS - Merge. Output File name: output.csv")
        return "Output File name: output.csv"

    @staticmethod
    def handle_invalid_flight(invalid_flights):
        """
        Method to handle invalid flight numbers passed to lookup method.

        :param invalid_flights:
            List of invalid flight numbers.
        """
        invalid_flight_response = []
        for invalid_flight_number in invalid_flights:
            invalid_flight_response.append(
                {
                    "flight_number": f"{invalid_flight_number}",
                    "error": "Flight not found in schedule",
                }
            )
        return invalid_flight_response

    def fetch_info(self, args):
        """
        Method to fetch/generate information based on command and arguments passed.

        :param args:
            List of arguments containing command and flight numbers if required.
        """
        try:
            command = args[1].lower()

            if command == AvailableInquiryCommands.LOOKUP.value:
                try:
                    flight_numbers = args[2].split(",")
                    return self.lookup(flight_numbers)
                except IndexError:
                    msg = "No flight number(s) passed."
                    self._logger.warning(msg)
                    return msg
            elif command == AvailableInquiryCommands.MERGE.value:
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
