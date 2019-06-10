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
    socrata_class_objects_list = []
    socrata_counter = itertools.count()
    arcgisonline_counter = itertools.count()
    credentials_parser = Utility.setup_config(cfg_file=var.credentials_config_file_path)

    # CLASSES
    # FUNCTIONS
    # FUNCTIONALITY

    # TODO: Need a socrata client for making requests for protected information
    client_socrata = Utility.create_socrata_client(domain=var.md_open_data_domain,
                                                   app_token=credentials_parser["SOCRATA"]["app_token"],
                                                   username=credentials_parser["SOCRATA"]["username"],
                                                   password=credentials_parser["SOCRATA"]["password"])

    # TODO: Get the data.json from Socrata so have an inventory of all public datasets
    response_socrata = Utility.request_GET(url=var.md_socrata_data_json)
    try:
        response_socrata_json = response_socrata.json()
    except json.decoder.JSONDecodeError as jde:
        print(f"Error decoding socrata response to json: {jde}")
        exit()
    else:
        datasets_json = response_socrata_json.get("dataset", {})

    # TODO: Need to create Socrata Dataset objects from the datasets json, and store objects in list for use.
    for json_obj in datasets_json:
        print(next(socrata_counter))
        dataset_socrata = DatasetSocrata(dataset_json=json_obj)
        dataset_socrata.assign_data_json_to_class_values()
        dataset_socrata.extract_four_by_four()
        dataset_socrata.build_metadata_url()
        socrata_class_objects_list.append(dataset_socrata)

    # TODO: 



if __name__ == "__main__":
    main()
