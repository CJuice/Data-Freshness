"""

"""
import requests
import configparser
from sodapy import Socrata


class Utility:

    @staticmethod
    def request_GET(url, params=None):
        if params is None:
            params = {}

        try:
            response = requests.get(url=url, params=params)
        except Exception as e:
            print(f"Error with request to {url}. Code {response.status_code}. Error:{e}")
            exit()
        else:
            return response

    @staticmethod
    def setup_config(cfg_file: str) -> configparser.ConfigParser:
        """
        Instantiate the parser for accessing a config file.
        :param cfg_file: config file to access
        :return:
        """
        cfg_parser = configparser.ConfigParser()
        cfg_parser.read(filenames=cfg_file)
        return cfg_parser


