import os

from geopy.distance import geodesic

from exceptions.exceptions import BlobClientException


def cleanup_temp_files(*file_paths):
    """
    Delete temporary files

    :param file_paths:
        File path(s)
    """
    for x in file_paths:
        if os.path.exists(x):
            os.remove(x)


def download_blob_into_local_file(blob_client, file_name):
    """
    Download blob data and reads it into the temporary file.

    :param blob_client:
        Blob client connected to the blob store.
    :param file_name:
        Temporary filename to store the blob data.
    """
    try:
        with open(file_name, "wb") as f:
            blob_client.download_blob().readinto(f)
        return True
    except Exception as e:
        raise BlobClientException(
            f"Problem downloading blob file and replicating in local storage. error message: {e}"
        )


def get_distance_flown_in_nautical_miles(row):
    """
    Calculates the distance between source and destination airport in nautical miles.

    :param row:
        Joined row from the dataframe
    :returns: Distance in nautical miles
    """
    coordinate1 = (row.Lat, row.Lon)
    coordinate2 = (row.Lat_arrival, row.Lon_arrival)
    distance_in_nautical_miles = geodesic(coordinate1, coordinate2).nm
    return round(distance_in_nautical_miles, 2)


def reformat_datetime_to_string(row, format="%Y-%m-%d %H:%M:%S"):
    """
    Converts the datetime formatted row values to string in the given format.

    :param row:
        row from the dataframe
    :param format:
        New string format
    :returns: string formatted pandas series.
    """
    return row.apply(lambda x: x.strftime(format))
