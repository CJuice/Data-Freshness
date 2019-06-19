"""

"""
import DataFreshness.doit_DataFreshness_Variables as var
from DataFreshness.doit_DataFreshness_Utility import Utility
import json


class DatasetArcGISOnline:
    """

    """
    OWNER = 'owner:mdimapdatacatalog'
    RECORD_LIMIT = '100'
    SORT_FIELD = 'title'

    def __init__(self):
        pass

    # def build_arcgis_request_data_dict(self, start_num: int = None) -> dict:
    #     return {
    #         'q': DatasetArcGISOnline.OWNER,
    #         'num': DatasetArcGISOnline.RECORD_LIMIT,
    #         'start': start_num,
    #         'sortField': DatasetArcGISOnline.SORT_FIELD,
    #         'f': 'json'
    #     }
    @staticmethod
    def request_and_aggregate_all_data_catalog_records(url: str, start_num: int = None) -> list:
        """
        Make web requests and accumulate records until no more are returned for the dataset of interest

        :return: returns a list of json/dicts for datasets
        """
        more_records_exist_than_response_limit_allows = True
        total_record_count = 0
        record_offset_value = 0
        master_list_of_dicts = []
        while more_records_exist_than_response_limit_allows:
            request_cycle_record_count = 0
            data = {
                'q': DatasetArcGISOnline.OWNER,
                'num': DatasetArcGISOnline.RECORD_LIMIT,
                'start': start_num,
                'sortField': DatasetArcGISOnline.SORT_FIELD,
                'f': 'json'
            }
            response = Utility.request_POST(url=url, data=data)
            try:
                resp_json = response.json()
            except json.JSONDecodeError as jse:
                print(f"JSONDecodeError after making post request to arcgis online. url={url}, data={data} {jse}")
                exit()
            else:
                master_list_of_dicts.extend(resp_json.get("results"))

            number_of_records_returned = len(response)
            request_cycle_record_count += number_of_records_returned
            total_record_count += number_of_records_returned

            # Any cycle_record_count that equals the max limit indicates another request is needed
            if request_cycle_record_count == limit_max_and_offset:

                # Give Socrata servers small interval before requesting more
                time.sleep(0.2)
                record_offset_value = request_cycle_record_count + record_offset_value
            else:
                more_records_exist_than_response_limit_allows = False
        return master_list_of_dicts