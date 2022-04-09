import pandas as pd

from exceptions.exceptions import BlobClientException, BlobDataFetchException
from models.blob_storage import BlobStorage
from utils.common_utils import cleanup_temp_files
from utils.common_utils import download_blob_into_local_file


class DataService:
    """
    A class to interact with blob store.
    """

    def __init__(self):
        self.blob_storage = BlobStorage()

    def fetch_data_from_blob_store(self, container_name, blob_name):
        """
        Main function to create instance of blob client and then download the remote data to local temporary file.

        :param container_name:
            Describe blob container in the blob store.
        :param blob_name:
            Describe blob name inside the container.

        :returns: Pandas dataframe object with respective data.
        """
        data = pd.DataFrame()
        try:
            with self.blob_storage.get_client(
                container_name=container_name, blob_name=blob_name
            ) as blob_client:
                if download_blob_into_local_file(
                    blob_client=blob_client, file_name=blob_name
                ):
                    if blob_name.split(".")[-1] == "json":
                        data = pd.read_json(blob_name)
                    elif blob_name.split(".")[-1] == "csv":
                        data = pd.read_csv(blob_name)
        except BlobClientException as msg:
            return BlobDataFetchException(msg)
        finally:
            cleanup_temp_files(blob_name)
        return data
