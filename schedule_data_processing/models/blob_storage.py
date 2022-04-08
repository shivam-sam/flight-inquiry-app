from azure.storage.blob import BlobClient
from .configuration_manager import ConfigManager
from exceptions.exceptions import BlobClientException


class BlobStorage:
    """
    A class to create blob client using connection url.
    """

    def __init__(self):
        self.__default_endpoints_protocol = ConfigManager.get("blob_store", "default_endpoints_protocol")
        self.__account_name = ConfigManager.get("blob_store", "account_name")
        self.__account_key = ConfigManager.get("blob_store", "account_key")
        self.__endpoint_suffix = ConfigManager.get("blob_store", "endpoint_suffix")

    def _get_connection_url(self):
        """
        Creates connection url string.
        :return: connection url string
        """
        return(f"DefaultEndpointsProtocol={self.__default_endpoints_protocol};AccountName={self.__account_name};"
               f"AccountKey={self.__account_key};EndpointSuffix={self.__endpoint_suffix}")

    def get_client(self, container_name, blob_name):
        """
        Creates blob client.
        :param str container_name:
            The name of the container.
        :param str blob_name:
            The name of the blob

        :return: blob client
        """
        try:
            client = BlobClient.from_connection_string(conn_str=self._get_connection_url(),
                                                       container_name=container_name, blob_name=blob_name)
            return client
        except Exception as e:
            msg = f"Error while creating blob client. Message: {e}"
            raise BlobClientException(msg)
