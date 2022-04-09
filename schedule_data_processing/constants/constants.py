import enum


class BlobStoreConstants(enum.Enum):
    CONTAINER_NAME = "python-case-study"
    SCHEDULE_BLOB_NAME = "schedule.json"
    FLEET_BLOB_NAME = "fleet.csv"
    AIRPORTS_BLOB_NAME = "airports.csv"


class FileExtensionConstants(enum.Enum):
    JSON_FILETYPE = "json"
    CSV_FILETYPE = "csv"


class FleetDataColumns(enum.Enum):
    REG = "Reg"
    AIRCRAFT_REGISTRATION = "aircraft_registration"
    IATA_TYPE_DESIGNATOR = "IATATypeDesignator"
    TYPE_NAME = "TypeName"
    TOTAL = "Total"
    HUB = "Hub"
    HAUL = "Haul"


class ScheduleDataColumns(enum.Enum):
    FLIGHT_NUMBER = "flight_number"
    SCHEDULED_DEPARTURE_TIME = "scheduled_departure_time"
    SCHEDULED_TAKEOFF_TIME = "scheduled_takeoff_time"
    SCHEDULED_LANDING_TIME = "scheduled_landing_time"
    SCHEDULED_ARRIVAL_TIME = "scheduled_arrival_time"


class AirportDataColumns(enum.Enum):
    AIRPORT = "Airport"
    DEPARTURE_AIRPORT = "departure_airport"
    ARRIVAL_AIRPORT = "arrival_airport"


class MergedDataColumns(enum.Enum):
    DISTANCE_IN_NM = "distance_nm"


class AvailableInquiryCommands(enum.Enum):
    LOOKUP = "lookup"
    MERGE = "merge"
