import pandas as pd
from models.blob_storage import BlobStorage
from utils.common_utils import cleanup_temp_files
from utils.common_utils import create_file


class DataService:

    def __init__(self):
        self.blob_storage = BlobStorage()

    def fetch_data_from_blob_store(self, container_name, blob_name):
        data = pd.DataFrame()
        blob_client = self.blob_storage.get_client(container_name=container_name, blob_name=blob_name)
        if create_file(file_name=blob_name, blob_client=blob_client):
            if blob_name.split(".")[-1] == "json":
                data = pd.read_json(blob_name)
            elif blob_name.split(".")[-1] == "csv":
                data = pd.read_csv(blob_name)
        cleanup_temp_files("schedule.json", "fleet.csv", "airports.csv")
        return data
