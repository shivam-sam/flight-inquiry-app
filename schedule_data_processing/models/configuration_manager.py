import os
import configparser


class ConfigManager:

    @staticmethod
    def _get_environment_setting(section, setting, default):
        config_settings = ConfigManager._get_config_setting()
        try:
            return config_settings.get(section=section, option=setting)
        except configparser.NoOptionError:
            return default

    @staticmethod
    def _get_config_filepath():
        environment_config = os.path.realpath(
            os.path.join(os.path.dirname(__file__), "../config/environment.ini")
        )
        return environment_config

    @staticmethod
    def _get_config_setting():
        config = configparser.ConfigParser()
        config_file = ConfigManager._get_config_filepath()
        config.read(config_file)
        return config

    @staticmethod
    def get(section, setting, default=None):
        return ConfigManager._get_environment_setting(section, setting, default)
