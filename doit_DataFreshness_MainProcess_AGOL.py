"""

"""


def main():
    import time
    start_time = time.time()
    print(f"Start Time: {start_time} seconds since Epoch")

    # IMPORTS
    import itertools
    import json
    import numpy as np
    import pandas as pd
    import pprint
    import requests

    from DataFreshness.doit_DataFreshness_Utility import Utility
    import DataFreshness.doit_DataFreshness_Variables_AGOL as var
    from DataFreshness.doit_DataFreshness_DatasetAGOL import DatasetAGOL
    # Disable the security warnings for https from data.maryland.gov
    requests.packages.urllib3.disable_warnings()
    print(f"\nImports Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")

    # VARIABLES
    agol_counter = itertools.count()
    agol_data_catalog_responses = []
    agol_class_objects_dict = {}

    # CLASSES
    # FUNCTIONS
    # FUNCTIONALITY
    print(f"\nVariablss Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")

    # ===================================================
    # ARCGIS ONLINE
    print(f"\nBeginning ArcGIS Online Process...")
    master_list_of_results = DatasetAGOL.request_all_data_catalog_results()

    # Print outs for general understanding of data.json level process
    print(f"Data Catalog Results Requests Process Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")
    print(f"{len(master_list_of_results)} results collected")

    for result in master_list_of_results:
        agol_dataset = DatasetAGOL()
        agol_dataset.assign_data_catalog_json_to_class_values(data_json=result)
        agol_dataset.build_standardized_item_url()

        # Store the objects for use
        agol_class_objects_dict[agol_dataset.id] = agol_dataset

    # Need to request the metadata xml for each object, handle the xml, extract values and assign them to the object
    for item_id, agol_dataset in agol_class_objects_dict.items():
        agol_dataset.build_metadata_xml_url()
        metadata_response = Utility.request_POST(url=agol_dataset.metadata_url)
        if 300 < metadata_response.status_code:
            print(f"ISSUE: AGOL Item {agol_dataset.standardized_url} metadata url request response is {metadata_response.status_code}. Resource skipped.")
            continue




    # ===================================================


if __name__ == "__main__":
    main()
