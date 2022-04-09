import sys

try:
    from services.driver_service import DriverService
except ImportError:
    from schedule_data_processing.services.driver_service import DriverService


def main(args):
    """Main function to create instance of the driver class."""
    driver_service_obj = DriverService()
    return driver_service_obj.fetch_info(args)


if __name__ == "__main__":
    # print(main(['schedule_data_processing/app.py', 'lookup', 'ZG2361,ZG5001,AAAAAA,CCCCC']))
    print(main(sys.argv))
