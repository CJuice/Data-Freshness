"""
Main processing script coordinating all functionality for completing data freshness process.
"""


def main():
    import time
    start_time = time.time()
    print(f"Start Time: {start_time} seconds since Epoch")

    # IMPORTS
    import configparser
    import datetime
    import itertools
    import json
    import numpy as np
    import os
    import pandas as pd
    import pprint
    import sodapy

    from DataFreshness.doit_DataFreshness_DatasetSocrata import DatasetSocrata
    from DataFreshness.doit_DataFreshness_Utility import Utility
    import DataFreshness.doit_DataFreshness_Variables as var
    print(f"\nImports Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")

    # VARIABLES

    arcgisonline_counter = itertools.count()
    asset_inventory_url = None
    boolean_string_replacement_dict = {"true": True, "false": False}
    credentials_parser = Utility.setup_config(cfg_file=var.credentials_config_file_path)
    socrata_assetinventory_counter = itertools.count()
    socrata_assetinventory_non_public_dataset_counter = itertools.count()
    socrata_assetinventory_public_dataset_counter = itertools.count()
    socrata_class_objects_dict = {}
    socrata_displaytype_map_counter = itertools.count()
    socrata_datajson_counter = itertools.count()
    socrata_datajson_object_counter = itertools.count()
    socrata_gis_dataset_counter = itertools.count()
    socrata_metadata_counter = itertools.count()

    print(f"\nVariablss Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")

    # CLASSES
    # FUNCTIONS
    # FUNCTIONALITY

    # ===================================================
    # SOCRATA
    print(f"\nBeginning Socrata Process...")
    # Need a socrata client for making requests for protected information
    DatasetSocrata.SOCRATA_CLIENT = DatasetSocrata.create_socrata_client(domain=var.md_open_data_domain,
                                                                         app_token=credentials_parser["SOCRATA"][
                                                                             "app_token"],
                                                                         username=credentials_parser["SOCRATA"][
                                                                             "username"],
                                                                         password=credentials_parser["SOCRATA"][
                                                                             "password"])

    # Get the data.json from Socrata so have an inventory of all public datasets
    # Did no design to handle iterative requests for record count greater than limit. It appears to send all datasets
    #   in initial request.
    response_socrata = Utility.request_GET(url=var.md_socrata_data_json_url)

    try:
        response_socrata_json = response_socrata.json()
    except json.decoder.JSONDecodeError as jde:
        print(f"Error decoding socrata response to json: {jde}")
        exit()
    else:
        socrata_data_json = response_socrata_json.get("dataset", {})

    # Create Socrata Dataset class objects from the datasets json, and store objects in list for use.
    # Add filtering for 'MD iMAP:', 'Dataset Freshness', 'Homepage Categories"
    for json_obj in socrata_data_json:
        next(socrata_datajson_counter)

        # instantiate object and assign values
        dataset_socrata = DatasetSocrata()
        dataset_socrata.assign_data_json_to_class_values(dataset_json=json_obj)

        # Before storing obj in dict see that it passes the exclusion filter
        # FIXME: Seeing two dataset freshness datasets. Determine if valid or is an issue
        if not dataset_socrata.passes_filter_data_json(gis_counter=socrata_gis_dataset_counter):
            continue

        # proceed with processing and store object for use
        dataset_socrata.extract_four_by_four()
        dataset_socrata.build_metadata_url()
        dataset_socrata.build_resource_url()
        socrata_class_objects_dict[dataset_socrata.four_by_four] = dataset_socrata
        next(socrata_datajson_object_counter)

    # Print outs for general understanding of data.json level process
    print(f"\nData.json Process Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")
    print(f"Number of data.json datasets handled: {socrata_datajson_counter}")
    print(f"Number of data.json GIS Datasets encountered: {socrata_gis_dataset_counter}")
    print(f"Number of data.json dataset objects created. {socrata_datajson_object_counter}")

    # Get all asset inventory information, and then store values in existing dataset objects using the 4x4
    asset_inventory_json_data_list = DatasetSocrata.request_and_aggregate_all_socrata_records(
        client=DatasetSocrata.SOCRATA_CLIENT,
        fourbyfour=credentials_parser['SOCRATA']['asset_inventory_fourbyfour'])

    for asset_json_obj in asset_inventory_json_data_list:
        next(socrata_assetinventory_counter)

        # exchange json 'true' & 'false' for python True & False
        public_raw = asset_json_obj.get("public", None)
        public = boolean_string_replacement_dict.get(public_raw, None)

        # Filter out datasets where public is not True
        if public is None:
            pprint.pprint(f"Issue extracting 'public' from asset inventory json. Skipped: {asset_json_obj}")
            continue
        elif public is True:
            # These are the datasets of interest
            next(socrata_assetinventory_public_dataset_counter)
            pass
        elif public is False:
            next(socrata_assetinventory_non_public_dataset_counter)
            continue
        else:
            print(f"Unexpected 'public' value extracted from asset inventory json: {public}")
            exit()

        # For public datasets, extract u_id and use that to get corresponding data.json based Socrata dataset object
        u_id = asset_json_obj.get("u_id", None)
        if u_id is None:
            pprint.pprint(f"Issue extracting 'u_id'' from asset inventory json. Skipped: {asset_json_obj}")
            continue
        else:
            # Objects in the data.json are public and visible. If a u_id is not in the dict of objects created from
            #   the data.json then it is likely not public. But, filtered above anyway so as to be explicit about it.
            socrata_data_obj = socrata_class_objects_dict.get(u_id, None)

        if socrata_data_obj is None:
            continue
        else:
            # print(obj.get("name", None), socrata_data_obj.title)
            socrata_data_obj.assign_asset_inventory_json_to_class_values(asset_json=asset_json_obj)

    # Print outs for general understanding of asset inventory level process
    print(f"\nAsset Inventory Process Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")
    print(f"Number of asset inventory datasets handled: {socrata_assetinventory_counter}")
    print(f"Number of public datasets encountered: {socrata_assetinventory_public_dataset_counter}")
    print(f"Number of non-public datasets encountered: {socrata_assetinventory_non_public_dataset_counter}")

    # Now that have all values from data.json and asset inventory...
    for fourbyfour, dataset_obj in socrata_class_objects_dict.items():
        next(socrata_metadata_counter)
        metadata_json = DatasetSocrata.SOCRATA_CLIENT.get_metadata(dataset_identifier=fourbyfour,
                                                                   content_type="json")
        dataset_obj.assign_metadata_json_to_class_values(metadata_json=metadata_json)

    key_and_displaytype_list = [(key, value.display_type) for key, value in socrata_class_objects_dict.items()]
    for key, display_type in key_and_displaytype_list:
        if display_type.lower() == "map":
            result = socrata_class_objects_dict.pop(key, None)
            if result is not None:
                next(socrata_gis_dataset_counter)
                next(socrata_displaytype_map_counter)
            else:
                print(f"Error, object {key} unsuccessfully deleted after detecting metadata displayType = map.")

    print(f"\nMetadata Process Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")
    print(f"Number of metadata datasets handled: {socrata_metadata_counter}")
    print(f"Number of displayType = 'map' objects deleted from dataset invntory: {socrata_displaytype_map_counter}")
    print(f"Number of GIS Datasets encountered has been updated: {socrata_gis_dataset_counter}")
    print(f"Number of remaining public Socrata dataset objects: {len(socrata_class_objects_dict)}")

    # Check the displayType values
    display_type_values_set = set()
    for key, obj in socrata_class_objects_dict.items():
        display_type_values_set.add(obj.display_type)
        if obj.display_type.lower() == "map":
            print(obj.four_by_four)
    # print(f"Datasets where displayType = 'map' have been deleted. Remaining values detected: {display_type_values_set}")

    # Need a master pandas dataframe from all remaining Socrata datasets
    df_data = [pd.Series(data=data_obj.__dict__) for data_obj in socrata_class_objects_dict.values()]
    master_socrata_df = pd.DataFrame(data=df_data,
                                     # index=list(socrata_class_objects_dict.keys()),
                                     # columns=df_columns,
                                     dtype=None,
                                     copy=False)
    print(f"\nSocrata DataFrame Creation Process Completed... {Utility.calculate_time_taken(start_time=start_time)} seconds since start")
    print(master_socrata_df.info())

    # TODO: Need to convert field types, process values such as dates, calculate values, build attributes, etc
    # Use rows_updated_by four-by-four value to build socrata profile url
    master_socrata_df["rows_updated_by"] = master_socrata_df["rows_updated_by"].apply(lambda value: var.md_socrata_profile_url.format(root=var.md_open_data_url, user_four_by_four=value))

    # TODO: Need to match existing data freshness output and write json and excel files for all objects
    # For full production version can write Socrata and AGOL at same time but for testing can just output Socrata now
    master_socrata_df.to_excel(excel_writer=var.output_excel_file_path,
                               sheet_name=var.output_excel_sheetname,
                               na_rep=np.NaN,
                               float_format=None,
                               columns=,  # TODO
                               header=var.output_report_headers,
                               index=,
                               index_label=)

    # ===================================================
    # ARCGIS ONLINE

    # ===================================================


if __name__ == "__main__":
    main()
