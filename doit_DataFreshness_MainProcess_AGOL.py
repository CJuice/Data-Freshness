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
    master_set = set()
    for item_id, agol_dataset in agol_class_objects_dict.items():
        agol_dataset.build_metadata_xml_url()
        metadata_response = Utility.request_POST(url=agol_dataset.metadata_url)
        if 300 < metadata_response.status_code:
            print(f"ISSUE: AGOL Item {agol_dataset.standardized_url} metadata url request response is {metadata_response.status_code}. Resource skipped. Solution has been to go to AGOL and publish the metadata.")
            continue

        # TODO: Need to handle sml and parse to usable form
        metadata_xml_element = Utility.parse_xml_response_to_element(response_xml_str=metadata_response.text)

        # Need to determine overlap of values between metadata content and data catalog content, document decisions on choices of values and source
        # TODO: Need to develop functionality to extract values from xml and assign to attributes in class objects.
        root_element = Utility.extract_first_immediate_child_feature_from_element(element=metadata_xml_element, tag_name=".")
        # print(list(root_element))
        # data_id_element_list = Utility.extract_all_immediate_child_features_from_element(element=metadata_xml_element, tag_name="dataIdInfo")
        # print(metadata_xml_element.find("Esri"))
        agol_dataset.esri_metadata_xml_element = Utility.extract_first_immediate_child_feature_from_element(element=root_element,
                                                                                                   tag_name="Esri")
        agol_dataset.extract_and_assign_esri_date_time_values()

        # continue
        # print(agol_dataset.meta_creation_date)
        # print(agol_dataset.meta_creation_time)
        # print(agol_dataset.meta_modification_date)
        # print(agol_dataset.meta_modification_time)
        # if esri_xml_element is not None:
        #     print("\n")
        #     print(Utility.extract_first_immediate_child_feature_from_element(element=esri_xml_element, tag_name="CreaDate").text)
        #     print(Utility.extract_first_immediate_child_feature_from_element(element=esri_xml_element, tag_name="CreaTime").text)
        #     print(Utility.extract_first_immediate_child_feature_from_element(element=esri_xml_element, tag_name="ModDate").text)
        #     print(Utility.extract_first_immediate_child_feature_from_element(element=esri_xml_element, tag_name="ModTime").text)
        #     print(Utility.extract_first_immediate_child_feature_from_element(element=esri_xml_element, tag_name="CreaDate").text)


    # ===================================================


if __name__ == "__main__":
    main()
