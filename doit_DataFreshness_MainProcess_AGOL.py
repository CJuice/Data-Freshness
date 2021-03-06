"""
Main process for ArcGIS Online asset examinations.
This process makes requests to the Maryland ArcGIS online organizational account for all assets. This information
is processed and assigned to attributes in a class. The dataset objects are then used to store data extracted from
a metadata xml request and a agol groups request. Not all of the attributes that are stored are used at this time. In
addition, there are attributes and extraction functionality that are commented out. The idea was to build a cadillac
now and grow into the abundancy of features rather than have to come back later and add features onto a pinto.
Author: CJuice
Date: 20190702
Revisions:
20190708, CJuice, Changed handling of response. Was making request and calling .json() on it and then chained a .get().
    Sometimes the response was None and the .json() raised exception. Undid the chaining and made separate calls with
    tertiary statement check for None
20190712, CJuice: Implemented multi-threading for metadata, groups, record count and dataset fields web requests
    process. Reduced time significantly. deployed to server and ran successfully.
    20201123, CJuice, added conversion of pandas datetime with timezone awareness to date. Upgrade of pandas
        produced the issue.

"""


def main():

    # IMPORTS
    import time
    start_time = time.time()
    print(f"Start Time: {start_time} seconds since Epoch")

    import itertools
    import json
    import numpy as np
    import pandas as pd
    import requests
    import DataFreshness.doit_DataFreshness_Variables_AGOL as var

    from DataFreshness.doit_DataFreshness_DatasetAGOL import DatasetAGOL
    from DataFreshness.doit_DataFreshness_GroupAGOL import GroupAGOL
    from DataFreshness.doit_DataFreshness_Utility import Utility

    # Disable the security warnings for https from data.maryland.gov
    requests.packages.urllib3.disable_warnings()

    print(f"\nImports Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")

    # VARIABLES
    agol_class_objects_dict = {}
    agol_dataset_counter = itertools.count()
    agol_group_objects_dict = {}
    agol_metadata_counter = itertools.count()
    agol_number_of_rows_counter = itertools.count()
    agol_other_counter = itertools.count()
    agol_webapp_counter = itertools.count()
    agol_webmap_counter = itertools.count()
    output_full_dataframe = True
    skipped_assets_counter_dict = {"Web Map": agol_webmap_counter, "Web Mapping Application": agol_webapp_counter}
    print(f"\nVariables Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")

    # FUNCTIONALITY
    print(f"\nArcGIS Online Process Initiating...")
    master_list_of_results = DatasetAGOL.request_all_data_catalog_results()

    # Print outs for general understanding of data.json level process
    print(f"Data Catalog Results Requests Process Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")
    print(f"{len(master_list_of_results)} results collected")

    for result in master_list_of_results:
        agol_dataset = DatasetAGOL()
        agol_dataset.assign_data_catalog_json_to_class_values(data_json=result)
        agol_dataset.build_standardized_item_url()
        agol_dataset.create_tags_string()
        agol_dataset.check_for_null_source_url_and_replace()
        agol_dataset.build_metadata_xml_url()

        # Check type_ to eliminate those we are not interested in evaluating
        if agol_dataset.type_ not in var.types_to_evaluate:
            next(skipped_assets_counter_dict.get(agol_dataset.type_, agol_other_counter))
            continue

        # Store the objects for use
        agol_class_objects_dict[agol_dataset.id] = agol_dataset
        next(agol_dataset_counter)

    # Print outs for general understanding of data catalog process
    print(f"\nData Catalog Process Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")
    print(f"Number of data layers encountered: {agol_dataset_counter}")
    print(f"Number of web maps encountered: {agol_webmap_counter}")
    print(f"Number of web apps encountered: {agol_webapp_counter}")
    print(f"Number of other items encountered: {agol_other_counter}")

    # Need to request the metadata xml for each object, handle the xml, extract values and assign them to the object
    print(f"\nMetadata Process Initiating...")

    # Multi-threading implementation:
    #   Need a list of tuples that can be iterated over by map function in download all sites
    metadata_details_tuples_list = [(item_id,
                                     agol_dataset.metadata_url,
                                     None) for item_id, agol_dataset in agol_class_objects_dict.items()]
    metadata_threading_results_generator = Utility.download_all_sites(site_detail_tuples_list=metadata_details_tuples_list,
                                                                      func=Utility.download_site)

    for item_id, metadata_response in metadata_threading_results_generator:
        agol_dataset = agol_class_objects_dict.get(item_id)

        if 300 <= metadata_response.status_code:
            print(f"ISSUE: AGOL Item {item_id} metadata url request response is {metadata_response.status_code}. Resource skipped. Solution has been to go to AGOL and publish the metadata.")
            continue

        # Need to handle xml and parse to usable form
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

    # Need to make the requests to the Groups url to gather that value for processing
    print(f"\nGroups Process Initiating...")

    # Multi-threading implementation:
    #   Need a list of tuples that can be iterated over by map function in download all sites
    groups_details_tuples_list = [(item_id,
                                   var.arcgis_group_url.format(arcgis_items_root_url=var.arcgis_items_root_url,
                                                               item_id=agol_dataset.id),
                                   var.json_param_for_request) for item_id, agol_dataset in agol_class_objects_dict.items()]  #[0:5]
    groups_threading_results_generator = Utility.download_all_sites(site_detail_tuples_list=groups_details_tuples_list,
                                                                    func=Utility.download_site)
    for item_id, groups_response in groups_threading_results_generator:
        agol_dataset = agol_class_objects_dict.get(item_id)

        # these groups warrant an independent object/class. They are not part of the asset but something
        #   to which the asset can belong. Currently only use one value but in future may want to exploit more.
        group_dataset = GroupAGOL()
        group_dataset.assign_group_json_to_class_values(group_json=groups_response.json())

        # Appear to be about 23 unique groups 20190625 CJuice. little bit of waste rebuilding and overwriting but meh.
        #   Had done a test using a set to see how many unique ones there were at time of design.
        agol_group_objects_dict[group_dataset.group_id] = group_dataset

        # Store group_id in the agol_dataset attributes so can retrieve info from approp group based off id if necessary
        agol_dataset.group_id = group_dataset.group_id

        # Process the title string to extract the category value per the old data freshness design
        agol_dataset.process_category_from_group_object(group_object_title=group_dataset.group_title)

    print(f"\nGroups Process Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")

    # Need to get the number of rows in each dataset, and the column names for each dataset
    print(f"\nNumber of Rows Process Initiating...")

    # Multi-threading implementation:
    #   Need a list of tuples that can be iterated over by map function in download all sites
    rowcount_fields_details_tuples_list = [(item_id,
                                            var.root_service_query_url.format(data_source_rest_url=agol_dataset.url),
                                            var.record_count_params,
                                            var.fields_query_params) for item_id, agol_dataset in agol_class_objects_dict.items()]  #[0:5]
    threading_results_tuples_generator = Utility.download_all_sites(site_detail_tuples_list=rowcount_fields_details_tuples_list,
                                                                    func=Utility.download_site_rows_columns)

    # Need to iterate over results and assign to objects
    for item_id, record_count_response, field_query_response in threading_results_tuples_generator:
        agol_dataset = agol_class_objects_dict.get(item_id)
        try:
            response_json = record_count_response.json()
        except json.decoder.JSONDecodeError as jde:
            print(f"JSONDecodeError Exception raised during .json() call on number of rows query response: {agol_dataset.url_agol_item_id}")
            agol_dataset.number_of_rows = -9999
        else:
            agol_dataset.number_of_rows = response_json.get("count", -9999)

        agol_dataset.extract_and_assign_field_names(response=field_query_response)
        if next(agol_number_of_rows_counter) % 100 == 0:
            print(f"\tRounds of requests: {agol_number_of_rows_counter}. {Utility.calculate_time_taken(start_time=start_time)} seconds since start")

    print(f"\nNumber of Rows Process Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")

    # Need a master pandas dataframe from all agol datasets
    print(f"\nAGOL Dataframe Creation Process Initiating...")
    df_data = [pd.Series(data=data_obj.__dict__) for data_obj in agol_class_objects_dict.values()]
    master_agol_df = pd.DataFrame(data=df_data,
                                  dtype=None,
                                  copy=False)
    master_agol_df = master_agol_df.reindex(sorted(master_agol_df.columns), axis=1)

    # Need to address pandas issue with writing dates that are timezone aware to excel
    master_agol_df["publication_date_dt"] = pd.to_datetime(master_agol_df["publication_date_dt"], utc=True).dt.date

    master_agol_df.fillna(value=var.null_string, inplace=True)
    print(master_agol_df.info())
    print(f"\nAGOL DataFrame Creation Process Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")

    # Need to output a dataframe that matches the existing data freshness report
    master_agol_df.to_excel(excel_writer=var.output_excel_file_path_data_freshness_AGOL,
                            sheet_name=var.output_excel_sheetname,
                            na_rep=np.NaN,
                            float_format=None,
                            columns=list(var.dataframe_to_header_mapping_for_excel_output.values()),
                            header=list(var.dataframe_to_header_mapping_for_excel_output.keys()),
                            index=False)

    # Need to output json for the DataCompiled.json build
    json_output_df = master_agol_df[var.json_output_columns_list]
    json_output_df.to_json(path_or_buf=var.output_json_file_path_data_freshness_AGOL, orient="records")

    # For outputting the full dataframe
    if output_full_dataframe:
        master_agol_df.to_excel(excel_writer=var.output_excel_file_path_full_dataframe,
                                sheet_name=var.output_excel_sheetname,
                                na_rep=np.NaN,
                                float_format=None,
                                index=False)
        print(f"Full dataframe output to {var.output_excel_file_path_full_dataframe}")

    print(f"\nProcess Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")


if __name__ == "__main__":
    main()
