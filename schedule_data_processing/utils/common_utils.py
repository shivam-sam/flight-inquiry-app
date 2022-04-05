import os


def cleanup_temp_files(*file_paths):
    for x in file_paths:
        if os.path.exists(x):
            os.remove(x)


def create_file(file_name, blob_client, mode="wb"):
    try:
        with open(file_name, mode) as f:
            blob_client.download_blob().readinto(f)
        return True
    except:
        raise Exception("Problem writing to file")
