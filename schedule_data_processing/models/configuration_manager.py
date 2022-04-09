import configparser
import os


class ConfigManager:
    """
    Class to interact with the config setting.
    """

    @staticmethod
    def _get_environment_setting(section, setting, default):
        """
        Fetches value declared in config setting.

        :param str section:
            Refers to the section under which the setting is declared.
        :param str setting:
            Refers to the setting which needs to be fetched.
        :param str default:
            Refers to the default value to be passed if specified setting is not found.

        :return: config value or default value
        """
        config_settings = ConfigManager._get_config_setting()
        try:
            return config_settings.get(section=section, option=setting)
        except configparser.NoOptionError:
            return default

    @staticmethod
    def _get_config_filepath():
        """
        Fetches config setting file path.

        :return: config file path string
        """
        environment_config = os.path.realpath(
            os.path.join(os.path.dirname(__file__), "../config/environment.ini")
        )
        return environment_config

    @staticmethod
    def _get_config_setting():
        """
        Creates and return config parser object and reads the config file.

        :return: config parser object
        """
        config = configparser.ConfigParser()
        config_file = ConfigManager._get_config_filepath()
        config.read(config_file)
        return config

    @staticmethod
    def get(section, setting, default=None):
        """
        Returns the config setting or default based on config file.

        :return: config setting value
        """
        return ConfigManager._get_environment_setting(section, setting, default)
