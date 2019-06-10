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

    @staticmethod
    def create_socrata_client(username:str, password: str, app_token: str, domain: str, dataset_key: str) -> Socrata:
        """
        Create and return a Socrata client for use.

        NOTE_1: It seems absolutely essential the the domain be a domain and not a url; 'https://opendata.maryland.gov'
            will not substitute for 'opendata.maryland.gov'.

        :param cfg_parser: config file parser
        :param dataset_key: the section key of interest
        :param domain: domain for maryland open data portal.
        :return: Socrata connection client
        """

        return Socrata(domain=domain, app_token=app_token, username=username, password=password)
