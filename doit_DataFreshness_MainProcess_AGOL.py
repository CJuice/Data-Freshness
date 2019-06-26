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


        # # TESTING
        # items_of_interest = ["e108937848a3467292971c61c905c358", "b0dbba215755439ab4fb9741ce83b15b", "f6eb3680c6134348a3ab351f44cb6de3"]
        # if agol_dataset.id not in items_of_interest:
        #     continue

        # Store the objects for use
        agol_class_objects_dict[agol_dataset.id] = agol_dataset

        # if 100 < next(test_limiter):
        #     break

    # Need to request the metadata xml for each object, handle the xml, extract values and assign them to the object
    for item_id, agol_dataset in agol_class_objects_dict.items():
        agol_dataset.build_metadata_xml_url()
        metadata_response = Utility.request_POST(url=agol_dataset.metadata_url)

        if 300 <= metadata_response.status_code:
            print(f"ISSUE: AGOL Item {agol_dataset.url_agol_item_id} metadata url request response is {metadata_response.status_code}. Resource skipped. Solution has been to go to AGOL and publish the metadata.")
            continue

        # Need to handle sml and parse to usable form
        metadata_xml_element = Utility.parse_xml_response_to_element(response_xml_str=metadata_response.text)

        # Need to extract values from xml and assign to attributes in class objects.
        #   ESRI tags - CreaDate, CreaTime, ModDate, ModTime
        agol_dataset.extract_and_assign_esri_date_time_values(element=metadata_xml_element)

        #   pubDate tag (Publication Date)
        agol_dataset.extract_and_assign_publication_date(element=metadata_xml_element)

        #   rpOrgName tag (Organization Name)
        agol_dataset.extract_and_assign_organization_name(element=metadata_xml_element)

        #   MaintFreqCd tag (Maintenance Update Frequency)
        agol_dataset.extract_and_assign_maintenance_frequency_code(element=metadata_xml_element)
        agol_dataset.process_maintenance_frequency_code()

        # Perform processing, conversions, and eavaluations
        agol_dataset.convert_milliseconds_attributes_to_datetime()
        agol_dataset.parse_date_like_string_attributes()
        agol_dataset.parse_html_attribute_values_to_soup_get_text()
        agol_dataset.calculate_days_since_last_data_update()
        agol_dataset.is_up_to_date()

    # Need a master pandas dataframe from all agol datasets
    df_data = [pd.Series(data=data_obj.__dict__) for data_obj in agol_class_objects_dict.values()]
    master_agol_df = pd.DataFrame(data=df_data,
                                  dtype=None,
                                  copy=False)
    master_agol_df = master_agol_df.reindex(sorted(master_agol_df.columns), axis=1)
    print(f"\nAGOL DataFrame Creation Process Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")
    print(master_agol_df.info())

    # TODO: Need to convert field types, process values such as dates, calculate values, build attributes, etc
    master_agol_df.fillna(value=var.null_string, inplace=True)

    # TODO: Need to match existing data freshness output and write json and excel files for all objects
    master_agol_df.to_excel(excel_writer=var.output_excel_file_path,
                            sheet_name=var.output_excel_sheetname,
                            na_rep=np.NaN,
                            float_format=None,
                            index=False)



    # ===================================================


if __name__ == "__main__":
    main()
