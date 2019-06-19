"""

"""
import DataFreshness.doit_DataFreshness_Variables_Socrata as var
from DataFreshness.doit_DataFreshness_Utility import Utility
import json


class DatasetAGOL:
    """

    """
    OWNER = 'owner:mdimapdatacatalog'
    RECORD_LIMIT = '100' # When changed from 100 the return quanity doesn't actually change. Unsure why ineffective.
    SORT_FIELD = 'title'

    def __init__(self):
        pass

    def build_arcgis_request_data_dict(self, start_num: int = None) -> dict:
        return {
            'q': DatasetAGOL.OWNER,
            'num': DatasetAGOL.RECORD_LIMIT,
            'start': start_num,
            'sortField': DatasetAGOL.SORT_FIELD,
            'f': 'json'
        }

    @staticmethod
    def request_all_data_catalog_results(url: str) -> list:
        """
        Make web requests and accumulate records until no more are returned for the dataset of interest

        :return: returns a list of json/dicts for datasets
        """
        more_records_exist = True
        # total_record_count = 0
        start_number = 0
        master_list_of_dicts = []

        while more_records_exist:
            # request_cycle_record_count = 0
            data = {'q': DatasetAGOL.OWNER,
                    'num': DatasetAGOL.RECORD_LIMIT,
                    'start': start_number,
                    'sortField': DatasetAGOL.SORT_FIELD,
                    'f': 'json'
                    }
            response = Utility.request_POST(url=url, data=data)
            try:
                resp_json = response.json()
            except json.JSONDecodeError as jse:
                print(f"JSONDecodeError after making post request to arcgis online. url={url}, data={data} {jse}")
                exit()
            else:
                results = resp_json.get("results", {})
                start_number = resp_json.get("nextStart", None)
                master_list_of_dicts.extend(results)
                print(f"Start Number: {start_number}")
                # number_of_records_returned = len(results)
                # request_cycle_record_count += number_of_records_returned
                # total_record_count += number_of_records_returned

            # AGOL nextStart equal to -1 must indicate end of records reached
            if start_number == -1:
                more_records_exist = False

        return master_list_of_dicts