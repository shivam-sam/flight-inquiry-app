from .data_service import DataService


class InquiryService:

    def __init__(self):
        self.data_service = DataService()
        self.__SCHEDULE = None
        self.__FLEET = None
        self.__AIRPORTS = None

    def set_schedule(self):
        self.__SCHEDULE = self.data_service.fetch_data_from_blob_store("python-case-study", "schedule.json")

    def set_fleet(self):
        self.__FLEET = self.data_service.fetch_data_from_blob_store("python-case-study", "fleet.csv")

    def set_airports(self):
        self.__AIRPORTS = self.data_service.fetch_data_from_blob_store("python-case-study", "airports.csv")

    def get_schedule(self):
        return self.__SCHEDULE

    def get_fleet(self):
        return self.__FLEET

    def get_airports(self):
        return self.__AIRPORTS

    def set_data(self):
        self.set_schedule()
        self.set_airports()
        self.set_fleet()
