"""
Main process for ArcGIS Online asset examinations.
This process makes requests to the Maryland ArcGIS online organizational account for all assets. This information
is processed and assigned to attributes in a class. The dataset objects are then used to store data extracted from
a metadata xml request and a agol groups request. Not all of the attributes that are stored are used at this time. In
addition, there are attributes and extraction functionality that are commented out. The idea was to build a cadillac
now and grow into the abundancy of features rather than have to come back later and add features onto a pinto.
Author: CJuice
Date: 20190702
Modifications:

"""


def main():

    # IMPORTS
    import time
    start_time = time.time()
    print(f"Start Time: {start_time} seconds since Epoch")

    import itertools
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
    agol_other_counter = itertools.count()
    agol_webapp_counter = itertools.count()
    agol_webmap_counter = itertools.count()
    output_for_dashboard = True
    output_full_dataframe = True
    skipped_assets_counter_dict = {"Web Map": agol_webmap_counter, "Web Mapping Application": agol_webapp_counter}
    print(f"\nVariablss Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")

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
    for item_id, agol_dataset in agol_class_objects_dict.items():
        agol_dataset.build_metadata_xml_url()
        metadata_response = Utility.request_POST(url=agol_dataset.metadata_url)

        if 300 <= metadata_response.status_code:
            print(f"ISSUE: AGOL Item {agol_dataset.url_agol_item_id} metadata url request response is {metadata_response.status_code}. Resource skipped. Solution has been to go to AGOL and publish the metadata.")
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
    for item_id, agol_dataset in agol_class_objects_dict.items():
        # these groups warrant an independent object/class. They are not part of the asset but something
        #   to which the asset can belong. Currently only use one value but in future may want to exploit more.
        groups_response = Utility.request_GET(url=var.arcgis_group_url.format(arcgis_items_root_url=var.arcgis_items_root_url,
                                                                              item_id=agol_dataset.id),
                                              params=var.json_param_for_request)
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
    for item_id, agol_dataset in agol_class_objects_dict.items():
        record_count_response = Utility.request_GET(url=var.root_service_query_url.format(data_source_rest_url=agol_dataset.url),
                                                    params=var.record_count_params)
        agol_dataset.number_of_rows = record_count_response.json().get("count", -9999) if record_count_response is not None else -9999

        field_query_response = Utility.request_GET(url=var.root_service_query_url.format(data_source_rest_url=agol_dataset.url),
                                                   params=var.fields_query_params)
        agol_dataset.extract_and_assign_field_names(response=field_query_response)

    print(f"\nNumber of Rows Process Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")

    # Need a master pandas dataframe from all agol datasets
    print(f"\nAGOL Dataframe Creation Process Initiating...")
    df_data = [pd.Series(data=data_obj.__dict__) for data_obj in agol_class_objects_dict.values()]
    master_agol_df = pd.DataFrame(data=df_data,
                                  dtype=None,
                                  copy=False)
    master_agol_df = master_agol_df.reindex(sorted(master_agol_df.columns), axis=1)
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

    # TODO: Write out the tally of asset types for dashboard use
    # if output_for_dashboard:
    #

    print(f"\nProcess Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")


if __name__ == "__main__":
    main()
