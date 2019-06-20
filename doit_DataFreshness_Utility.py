"""

"""
import requests
import configparser
import time


class Utility:

    @staticmethod
    def request_GET(url: str, params: dict = None) -> requests.models.Response:

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
    def request_POST(url: str, data: dict = None, verify: bool = False) -> requests.models.Response:
        if data is None:
            data = {}

        try:
            response = requests.post(url=url, data=data, verify=verify)
        except Exception as e:
            # TODO: Refine
            print(f"Error during post request to url:{url}, data:{data}: {e}")
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
    def calculate_time_taken(start_time: float) -> float:
        """
        Calculate the time difference between now and the value passed as the start time

        :param start_time: Time value representing start of processing
        :return: Difference value between start time and current time
        """
        return (time.time() - start_time)

