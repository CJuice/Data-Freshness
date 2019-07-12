"""
A class of static utility methods independent of the process type (socrata or agol).
Author: CJuice
Date: 20190702
Revisions:
    20190712, CJuice: Added multi-threading methods and supporting imports and vars

"""

import configparser
import requests
import time
import xml.etree.ElementTree as ET


class Utility:
    """
    Class of utility static methods for use in processing.
    """

    import concurrent.futures
    import requests
    import threading

    THREAD_LOCAL = threading.local()

    @staticmethod
    def calculate_time_taken(start_time: float) -> float:
        """
        Calculate the time difference between now and the value passed as the start time

        :param start_time: Time value representing start of processing
        :return: Difference value between start time and current time
        """
        return time.time() - start_time

    @staticmethod
    def download_all_sites(site_detail_tuples_list: list, func):
        """
        Thread management function used to pool workers and map function to data
        :param site_detail_tuples_list: tuple containing id, urls, and params
        :param func: function to be mapped to data. varies with process being run
        :return: results as generator
        """
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:  # tested between 5 and 10, ~7 seem best
            result = executor.map(func, site_detail_tuples_list)
        return result

    @staticmethod
    def download_site(site_detail_tuple: tuple) -> tuple:
        """
        Basic request of single url by worker
        :param site_detail_tuple: tuple of id, url, and params
        :return: tuple of id and response
        """
        agol_item_id, url, params = site_detail_tuple

        if not hasattr(Utility.THREAD_LOCAL, "session"):
            Utility.THREAD_LOCAL.session = requests.Session()

        session = Utility.THREAD_LOCAL.session

        with session.get(url=url, params=params) as response:
            result = response

        return agol_item_id, result

    @staticmethod
    def download_site_rows_columns(site_detail_tuple: tuple) -> tuple:
        """
        Two requests to same machine for row count query, and fields in dataset query
        :param site_detail_tuple:  id, url, params, params
        :return: tuple of id, count response, fields response
        """
        agol_item_id, service_query_url, record_count_params, field_query_params = site_detail_tuple

        if not hasattr(Utility.THREAD_LOCAL, "session"):
            Utility.THREAD_LOCAL.session = requests.Session()

        session = Utility.THREAD_LOCAL.session

        with session.get(url=service_query_url, params=record_count_params) as count_response:
            count_result = count_response

        with session.get(url=service_query_url, params=field_query_params) as fields_response:
            fields_result = fields_response

        return agol_item_id, count_result, fields_result

    @staticmethod
    def request_GET(url: str, params: dict = None) -> requests.models.Response:
        """
        Make a GET request to a web resource and return response.
        :param url: the url to which to make the request
        :param params: params to pass in the request
        :return:
        """
        if params is None:
            params = {}

        try:
            response = requests.get(url=url, params=params)
        except Exception as e:
            print(f"Error with request to {url}. Error:{e}")
            return requests.models.Response()
        else:
            return response

    @staticmethod
    def request_POST(url: str, data: dict = None, verify: bool = False) -> requests.models.Response:
        """
        Make a POST request for a web resource and return response
        :param url: the url to which to make the request
        :param data: data to be passed in request
        :param verify:
        :return:
        """
        if data is None:
            data = {}

        try:
            response = requests.post(url=url, data=data, verify=verify)
        except Exception as e:
            # TODO: Refine exception handling
            print(f"Error during post request to url:{url}, data:{data}: {e}")
            exit()
        else:
            return response

    @staticmethod
    def extract_all_immediate_child_features_from_element(element: ET.Element, tag_name: str):
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
            return None

    @staticmethod
    def extract_first_immediate_child_feature_from_element(element: ET.Element, tag_name: str):
        """Extract first immediate child feature from provided xml ET.Element based on provided tag name

        :param element: xml ET.Element to interrogate
        :param tag_name: name of desired tag
        :return: ET.Element of interest
        """

        try:
            return element.find(tag_name)
        except AttributeError as ae:
            print(f"AttributeError: Unable to extract '{tag_name}' from {element.text}: {ae}")
            return None

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