"""

"""


def main():
    import time
    start_time = time.time()
    print(f"Start Time: {start_time} seconds since Epoch")

    # IMPORTS
    import itertools
    import numpy as np
    import pandas as pd
    import requests

    from DataFreshness.doit_DataFreshness_Utility import Utility
    import DataFreshness.doit_DataFreshness_Variables_AGOL as var
    from DataFreshness.doit_DataFreshness_DatasetAGOL import DatasetAGOL
    # Disable the security warnings for https from data.maryland.gov
    requests.packages.urllib3.disable_warnings()
    print(f"\nImports Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")

    # VARIABLES
    agol_dataset_counter = itertools.count()
    agol_webmap_counter = itertools.count()
    agol_webapp_counter = itertools.count()
    agol_other_counter = itertools.count()
    agol_metadata_counter = itertools.count()
    skipped_assets_counter_dict = {"Web Map": agol_webmap_counter, "Web Mapping Application": agol_webapp_counter}
    # agol_data_catalog_responses = []
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

        # Check type_ to eliminate those we are not interested in evaluating
        if agol_dataset.type_ not in var.types_to_evaluate:
            next(skipped_assets_counter_dict.get(agol_dataset.type_, agol_other_counter))
            continue



        # TESTING
        if agol_dataset.id != "c8b282aed4c84bb5a8c991e1a9b7b30e":
            continue




        # Store the objects for use
        agol_class_objects_dict[agol_dataset.id] = agol_dataset
        next(agol_dataset_counter)

    # Print outs for general understanding of data catalog process
    print(f"\nData Catalog Process Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")
    print(f"Number of data layers handled: {agol_dataset_counter}")
    print(f"Number of web maps encountered: {agol_webmap_counter}")
    print(f"Number of web apps encountered: {agol_webapp_counter}")
    print(f"Number of other items encountered: {agol_other_counter}")

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
        next(agol_metadata_counter)

    # Print outs for general understanding of metadata process
    print(f"\nMetadata Process Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")
    print(f"Number of metadata requests handled: {agol_metadata_counter}")

    # TODO: Need to make the requests to the Groups url to gather that value for processing
    # TODO: Need to get the number of rows in each dataset, and the column names for each dataset
    for item_id, agol_dataset in agol_class_objects_dict.items():
        record_count_response = Utility.request_GET(url=var.root_service_query_url.format(data_source_rest_url=agol_dataset.url), params=var.record_count_params)
        field_query_response = Utility.request_GET(url=var.root_service_query_url.format(data_source_rest_url=agol_dataset.url), params=var.fields_query_params)
        agol_dataset.number_of_rows = record_count_response.json().get("count", -9999)
        agol_dataset.extract_and_assign_field_names(response=field_query_response)
        print(agol_dataset.number_of_rows)
        print(agol_dataset.column_names_string)
        exit()

    # Need a master pandas dataframe from all agol datasets
    df_data = [pd.Series(data=data_obj.__dict__) for data_obj in agol_class_objects_dict.values()]
    master_agol_df = pd.DataFrame(data=df_data,
                                  dtype=None,
                                  copy=False)
    master_agol_df = master_agol_df.reindex(sorted(master_agol_df.columns), axis=1)

    # TODO: Need to convert field types, process values such as dates, calculate values, build attributes, etc
    master_agol_df.fillna(value=var.null_string, inplace=True)

    print(f"\nAGOL DataFrame Creation Process Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")
    print(master_agol_df.info())

    # Need to output a dataframe that matches the existing data freshness report
    master_agol_df.to_excel(excel_writer=var.output_excel_file_path_data_freshness_AGOL,
                            sheet_name=var.output_excel_sheetname,
                            na_rep=np.NaN,
                            float_format=None,
                            columns=list(var.dataframe_to_header_mapping_for_excel_output.values()),
                            header=list(var.dataframe_to_header_mapping_for_excel_output.keys()),
                            index=False)

    # For outputting the full dataframe
    # master_agol_df.to_excel(excel_writer=var.output_excel_file_path_full_dataframe,
    #                         sheet_name=var.output_excel_sheetname,
    #                         na_rep=np.NaN,
    #                         float_format=None,
    #                         index=False)

    # ===================================================


if __name__ == "__main__":
    main()
