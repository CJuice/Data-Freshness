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
    arcgisonline_counter = itertools.count()
    asset_inventory_url = None
    credentials_parser = Utility.setup_config(cfg_file=var.credentials_config_file_path)
    socrata_class_objects_dict = {}
    socrata_counter = itertools.count()
    socrata_gis_dataset_counter = itertools.count()


    # CLASSES
    # FUNCTIONS
    # FUNCTIONALITY

    # ===================================================
    # SOCRATA
    # TODO: Need a socrata client for making requests for protected information
    DatasetSocrata.SOCRATA_CLIENT = DatasetSocrata.create_socrata_client(domain=var.md_open_data_domain,
                                                                         app_token=credentials_parser["SOCRATA"][
                                                                             "app_token"],
                                                                         username=credentials_parser["SOCRATA"][
                                                                             "username"],
                                                                         password=credentials_parser["SOCRATA"][
                                                                             "password"])

    # TODO: Get the data.json from Socrata so have an inventory of all public datasets
    # TODO: Redesign to handle more records than default limit, even if doesn't need it at this time
    response_socrata = Utility.request_GET(url=var.md_socrata_data_json_url)

    try:
        response_socrata_json = response_socrata.json()
    except json.decoder.JSONDecodeError as jde:
        print(f"Error decoding socrata response to json: {jde}")
        exit()
    else:
        socrata_data_json = response_socrata_json.get("dataset", {})

    # TODO: Create Socrata Dataset class objects from the datasets json, and store objects in list for use.
    for json_obj in socrata_data_json:
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
    asset_inventory_url = f"{var.md_open_data_url}/resource/{credentials_parser['SOCRATA']['asset_inventory_fourbyfour']}.json"



    # ===================================================
    # ARCGIS ONLINE

    # ===================================================

if __name__ == "__main__":
    main()
