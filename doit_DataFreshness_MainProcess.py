"""
Main processing script coordinating all functionality for completing data freshness process.
"""


def main():

    # IMPORTS
    import configparser
    import sodapy
    import pprint
    import json
    import itertools

    from DataFreshness.doit_DataFreshness_Utility import Utility
    from DataFreshness.doit_DataFreshness_DatasetSocrata import DatasetSocrata
    import DataFreshness.doit_DataFreshness_Variables as var
    import os


    # VARIABLES
    arcgisonline_counter = itertools.count()
    asset_inventory_url = None
    credentials_parser = Utility.setup_config(cfg_file=var.credentials_config_file_path)
    socrata_class_objects_dict = {}
    socrata_assetinventory_counter = itertools.count()
    socrata_assetinventory_non_public_dataset_counter = itertools.count()
    socrata_assetinventory_public_dataset_counter = itertools.count()
    socrata_datajson_counter = itertools.count()
    socrata_datajson_object_counter = itertools.count()
    socrata_gis_dataset_counter = itertools.count()
    boolean_string_replacement_dict = {"true": True, "false": False}


    # CLASSES
    # FUNCTIONS
    # FUNCTIONALITY

    # ===================================================
    # SOCRATA
    # Need a socrata client for making requests for protected information
    DatasetSocrata.SOCRATA_CLIENT = DatasetSocrata.create_socrata_client(domain=var.md_open_data_domain,
                                                                         app_token=credentials_parser["SOCRATA"][
                                                                             "app_token"],
                                                                         username=credentials_parser["SOCRATA"][
                                                                             "username"],
                                                                         password=credentials_parser["SOCRATA"][
                                                                             "password"])

    # Get the data.json from Socrata so have an inventory of all public datasets
    # TODO: Redesign to handle more records than default limit, even if doesn't need it at this time
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
        if not dataset_socrata.passes_filter(gis_counter=socrata_gis_dataset_counter):
            continue

        # proceed with processing and store object for use
        dataset_socrata.extract_four_by_four()
        dataset_socrata.build_metadata_url()
        dataset_socrata.build_resource_url()
        socrata_class_objects_dict[dataset_socrata.four_by_four] = dataset_socrata
        next(socrata_datajson_object_counter)

    print(f"Number of data.json datasets handled: {socrata_datajson_counter}")
    print(f"Number of data.json dataset objects created. {socrata_datajson_object_counter}")
    print(f"Number of GIS Datasets encountered: {socrata_gis_dataset_counter}")

    # TODO: Get all asset inventory information, and then store values in existing dataset objects using the 4x4
    # TODO: Get all asset inventory json, make a giant dictionary or even class objects, and
    #  then query locally to eliminate web transactions
    asset_inventory_data_list = DatasetSocrata.request_and_aggregate_all_socrata_records(
        client=DatasetSocrata.SOCRATA_CLIENT,
        fourbyfour=credentials_parser['SOCRATA']['asset_inventory_fourbyfour'])

    for obj in asset_inventory_data_list:
        next(socrata_assetinventory_counter)

        public_raw = obj.get("public", None)
        public = boolean_string_replacement_dict.get(public_raw, None)

        # Filter out datasets where public is not True
        if public is None:
            pprint.pprint(f"Issue extracting 'public' from obj json. Skipped: {obj}")
            continue
        elif public is True:
            next(socrata_assetinventory_public_dataset_counter)
            pass
        elif public is False:
            # print(f"Dataset 'public' status is False. Skipped.")
            next(socrata_assetinventory_non_public_dataset_counter)
            continue
        else:
            print(f"Unexpected 'public' value extracted from asset inventory json: {public}")
            exit()

        # For public datasets, extract u_id and use that to get corresponding data.json based Socrata dataset object
        u_id = obj.get("u_id", None)
        if u_id is None:
            pprint.pprint(f"Issue extracting 'u_id'' from obj json. Skipped: {obj}")
            continue
        else:
            # Objects in the data.json are public and visible. If a u_id is not in the dict of objects created from
            #   the data.json then it is likely not public
            existing_data_obj = socrata_class_objects_dict.get(u_id, None)

        if existing_data_obj is None:
            continue
        else:
            # print(obj.get("name", None), existing_data_obj.title)
            existing_data_obj.assign_asset_inventory_json_to_class_values(asset_json=obj)
    print(f"Number of asset inventory datasets handled: {socrata_assetinventory_counter}")
    print(f"Number of non-public datasets encountered: {socrata_assetinventory_non_public_dataset_counter}")
    print(f"Number of public datasets encountered: {socrata_assetinventory_public_dataset_counter}")

    # temp_set = set()
    # for obj in socrata_class_objects_dict.values():
    #     print(obj.title)
    # ===================================================
    # ARCGIS ONLINE

    # ===================================================

if __name__ == "__main__":
    main()
