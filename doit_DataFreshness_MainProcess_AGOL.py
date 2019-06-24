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
    test_limiter = itertools.count()

    for result in master_list_of_results:
        agol_dataset = DatasetAGOL()
        agol_dataset.assign_data_catalog_json_to_class_values(data_json=result)
        agol_dataset.build_standardized_item_url()

        # Store the objects for use
        agol_class_objects_dict[agol_dataset.id] = agol_dataset

        # if 100 < next(test_limiter):
        #     break

    # Need to request the metadata xml for each object, handle the xml, extract values and assign them to the object
    test_set_1 = set()
    test_set_2 = set()
    for item_id, agol_dataset in agol_class_objects_dict.items():
        agol_dataset.build_metadata_xml_url()
        metadata_response = Utility.request_POST(url=agol_dataset.metadata_url)

        if 300 < metadata_response.status_code:
            print(f"ISSUE: AGOL Item {agol_dataset.standardized_url} metadata url request response is {metadata_response.status_code}. Resource skipped. Solution has been to go to AGOL and publish the metadata.")
            continue

        # Need to handle sml and parse to usable form
        metadata_xml_element = Utility.parse_xml_response_to_element(response_xml_str=metadata_response.text)

        # Need to extract values from xml and assign to attributes in class objects.
        #   ESRI tags - CreaDate, CreaTime, ModDate, ModTime
        agol_dataset.esri_metadata_xml_element = Utility.extract_first_immediate_child_feature_from_element(
            element=metadata_xml_element,
            tag_name="Esri")
        agol_dataset.extract_and_assign_esri_date_time_values()

        #   pubDate tag (Publication Date)

        #   rpOrgName tag (Organization Name)
        #   MaintFreqCd tag (Maintenance Update Frequency)

        try:
            test_set_1.update([item.tag for item in list(metadata_xml_element.find("dataIdInfo").find("idCitation").find("citRespParty"))])
        except TypeError as te:
            print(agol_dataset.type_, te, agol_dataset.standardized_url)
            pass
        else:
            print(agol_dataset.type_, list(metadata_xml_element.find("dataIdInfo").find("idCitation").find("citRespParty")), agol_dataset.metadata_url)
            pass
        # test_set_2.update((agol_dataset.type_,))

    print(test_set_1)
    # print(test_set_2)

    # ===================================================


if __name__ == "__main__":
    main()
