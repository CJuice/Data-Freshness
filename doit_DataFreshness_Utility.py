"""

"""
import configparser
import xml.etree.ElementTree as ET
import requests
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

    @staticmethod
    def extract_all_immediate_child_features_from_element(element: ET.Element, tag_name: str) -> list:
        """
        Extract all immediate children of the element provided to the method.

        :param element: ET.Element of interest to be interrogated
        :param tag_name: tag of interest on which to search
        :return: list of all discovered ET.Element items
        """
        try:
            return element.findall(tag_name)
        except AttributeError as ae:
            print(f"AttributeError: Unable to extract '{tag_name}' from {element.text}: {ae}")
            exit()

    @staticmethod
    def extract_first_immediate_child_feature_from_element(element: ET.Element, tag_name: str) -> ET.Element:
        """Extract first immediate child feature from provided xml ET.Element based on provided tag name

        :param element: xml ET.Element to interrogate
        :param tag_name: name of desired tag
        :return: ET.Element of interest
        """

        try:
            return element.find(tag_name)
        except AttributeError as ae:
            print(f"AttributeError: Unable to extract '{tag_name}' from {element.text}: {ae}")
            exit()

    @staticmethod
    def parse_xml_response_to_element(response_xml_str: str) -> ET.Element:
        """
        Process xml response content to xml ET.Element
        :param response_xml_str: string xml from response
        :return: xml ET.Element
        """
        try:
            return ET.fromstring(response_xml_str)
        except Exception as e:  # TODO: Improve exception handling
            print(f"Unable to process xml response to Element using ET.fromstring(): {e}")
            exit()