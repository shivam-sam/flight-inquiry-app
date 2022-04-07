import os
from exceptions.exceptions import BlobClientException


def cleanup_temp_files(*file_paths):
    for x in file_paths:
        if os.path.exists(x):
            os.remove(x)


def download_blob_into_local_file(blob_client, file_name):
    try:
        with open(file_name, "wb") as f:
            blob_client.download_blob().readinto(f)
        return True
    except Exception as e:
        raise BlobClientException(f"Problem downloading blob file and replicating in local storage. error message: {e}")
