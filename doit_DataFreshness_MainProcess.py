"""
Main processing script coordinating all functionality for completing data freshness process.
"""


def main():

    # IMPORTS
    import configparser
    import sodapy
    from DataFreshness.doit_DataFreshness_Utility import Utility
    from DataFreshness.doit_DataFreshness_DatasetSocrata import DatasetSocrata
    import DataFreshness.doit_DataFreshness_Variables as var

    # VARIABLES
    socrata_class_objects_list = []

    # CLASSES
    # FUNCTIONS
    # FUNCTIONALITY
    # TODO:
    credentials_parser = Utility.setup_config(cfg_file=var.credentials_config_file_path)
    socrata_client = Utility.create_socrata_client(domain=var.md_open_data_domain,
                                                   app_token=credentials_parser, username=, password=)
    response_socrata = Utility.request_GET(url=DatasetSocrata.MD_OPEN_DATA_URL)
    response_socrata_json = response_socrata.json()

    for json_obj in response_socrata_json:
        socrata_obj = DatasetSocrata(dataset_json=json_obj)
        socrata_obj.assign_data_json_to_class_values()
        socrata_obj.extract_four_by_four()
        socrata_obj.build_metadata_url()


if __name__ == "__main__":
    main()
