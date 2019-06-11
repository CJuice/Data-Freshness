"""
Main processing script coordinating all functionality for completing data freshness process.
"""


def main():

    # IMPORTS
    import configparser
    import sodapy
    import json
    import itertools

    from DataFreshness.doit_DataFreshness_Utility import Utility
    from DataFreshness.doit_DataFreshness_DatasetSocrata import DatasetSocrata
    import DataFreshness.doit_DataFreshness_Variables as var
    import os

    # VARIABLES
    socrata_class_objects_dict = {}
    socrata_counter = itertools.count()
    arcgisonline_counter = itertools.count()
    socrata_gis_dataset_counter = itertools.count()
    credentials_parser = Utility.setup_config(cfg_file=var.credentials_config_file_path)

    # CLASSES
    # FUNCTIONS
    # FUNCTIONALITY

    # ===================================================
    # SOCRATA
    # TODO: Need a socrata client for making requests for protected information
    DatasetSocrata.SOCRATA_CLIENT = Utility.create_socrata_client(domain=var.md_open_data_domain,
                                                                  app_token=credentials_parser["SOCRATA"]["app_token"],
                                                                  username=credentials_parser["SOCRATA"]["username"],
                                                                  password=credentials_parser["SOCRATA"]["password"])

    # TODO: Get the data.json from Socrata so have an inventory of all public datasets
    response_socrata = Utility.request_GET(url=var.md_socrata_data_json_url)
    try:
        response_socrata_json = response_socrata.json()
    except json.decoder.JSONDecodeError as jde:
        print(f"Error decoding socrata response to json: {jde}")
        exit()
    else:
        datasets_json = response_socrata_json.get("dataset", {})

    # TODO: Create Socrata Dataset class objects from the datasets json, and store objects in list for use.
    for json_obj in datasets_json:
        print(next(socrata_counter))
        dataset_socrata = DatasetSocrata()
        dataset_socrata.assign_data_json_to_class_values(dataset_json=json_obj)
        dataset_socrata.extract_four_by_four()
        dataset_socrata.build_metadata_url()
        dataset_socrata.build_resource_url()

        socrata_class_objects_dict[dataset_socrata.four_by_four] = dataset_socrata

    # TODO: Need to check for title="MD iMAP" objects, increment gis counter, and I think delete object
    # TODO: Need to check for title="Dataset Freshness" objects and skip??
    # TODO: Need to check for title="Homepage Categories" objects and skip??


    # TODO: Get all asset inventory information, and then store values in existing dataset objects using the 4x4
    # TODO: Get all asset inventory json, make a giant dictionary or even class objects, and
    #  then query locally to eliminate web transactions
    dataset_socrata.build_resource_url(
        asset_inventory_fourbyfour=credentials_parser["SOCRATA"]["asset_inventory_fourbyfour"])

    # ===================================================
    # ARCGIS ONLINE

    # ===================================================

if __name__ == "__main__":
    main()
