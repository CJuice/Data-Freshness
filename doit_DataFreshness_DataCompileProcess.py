"""
Process for compiling Socrata results with ArcGIS Online results to make a single resource for upload to Socrata.

There are two outputs. An excel file is generated. This file is used to refresh the Socrata open data portal Data
Freshness dataset. A json file is generated for a search portal feature that was being developed by JCahoon but fell
into dormancy when she left.
Two files are necessary for this to work. The first is the Socrata results from the DataFreshness process and the
second is the AGOL results. These two files have the same columns. The records are mashed/compiled together to make
a single dataset.
Author: CJuice
Date: 20190702
Modifications:

"""


def main():

    # IMPORTS
    import datetime
    import json
    import os
    import pandas as pd
    import DataFreshness.doit_DataFreshness_Variables_Socrata as var
    from DataFreshness.doit_DataFreshness_Utility import Utility
    from DataFreshness.doit_DataFreshness_DatasetSocrata import DatasetSocrata

    # VARIABLES
    # NOT-DERIVED
    _root_file_path = os.path.dirname(__file__)
    agol_dataframe_excel = None
    agol_excel_file_name = "AGOL_data_freshness.xlsx"
    agol_json_file_name = "AGOL_data_freshness.json"
    compiled_json_file_name = "DataCompiled.json"
    data_file_dir = f"{_root_file_path}/DataFreshnessOutputs"
    null_string = "NULL"
    null_datetime_value = datetime.datetime.strptime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    null_integer_value = -9999
    null_url_value = "https://N.U.LL"
    # TODO: Too many hard coded references to these fields. Extract to single location as variables and then reference
    null_value_column_fillna_specs = {"Link": null_url_value,
                                      "Source URL": null_url_value,
                                      "Date of Most Recent Data Change": null_datetime_value,
                                      "Days Since Most Recent Data Change": null_integer_value,
                                      "Date of Most Recent Change (Data Change or Metadata Change)": null_datetime_value,
                                      "Days Since Last View Update": null_integer_value,
                                      "Number of Rows": null_integer_value}
    socrata_dataframe_excel = None
    socrata_excel_file_name = "SOCRATA_data_freshness.xlsx"
    socrata_json_file_name = "SOCRATA_data_freshness.json"

    # DERIVED
    combined_data_file_name_excel = f"{data_file_dir}\dataFreshness_{datetime.datetime.now().strftime('%Y_%m_%d')}.xlsx"
    combined_data_file_name_json = f"{data_file_dir}\{compiled_json_file_name}"
    credentials_parser = Utility.setup_config(cfg_file=var.credentials_config_file_path)

    # Need a socrata client for making requests for protected information
    DatasetSocrata.SOCRATA_CLIENT = DatasetSocrata.create_socrata_client(domain=var.md_open_data_domain,
                                                                         app_token=credentials_parser["SOCRATA"][
                                                                             "app_token"],
                                                                         username=credentials_parser["SOCRATA"][
                                                                             "username"],
                                                                         password=credentials_parser["SOCRATA"][
                                                                             "password"])

    # FUNCTIONALITY
    # Excel Portion. The output is pushed up to Socrata Data Freshness report dataset by an FME job
    for dirname, dirs, files in os.walk(data_file_dir):
        for file in files:
            if file == agol_excel_file_name:
                agol_dataframe_excel = pd.read_excel(io=os.path.join(dirname, file), na_values=null_string)
            elif file == socrata_excel_file_name:
                socrata_dataframe_excel = pd.read_excel(io=os.path.join(dirname, file), na_values=null_string)

    master_data_freshness_df_excel = pd.concat([agol_dataframe_excel, socrata_dataframe_excel])

    # Unsure why NULL values are dropped on read, some setting I'm missing, but have to fill before exporting.
    #   Performing two rounds of filling. First round hits specific fields that required special types.
    #   Second round fills any remaining empty fields with text NULL value.
    master_data_freshness_df_excel.fillna(value=null_value_column_fillna_specs, inplace=True)  # Targeted columns
    master_data_freshness_df_excel.fillna(value=null_string, inplace=True)  # All others (text fields)
    master_data_freshness_df_excel.to_excel(excel_writer=combined_data_file_name_excel,
                                            index=False)
    # print(master_data_freshness_df_excel.info())
    socrata_upsert_version = master_data_freshness_df_excel.astype(dtype={"Date of Most Recent Data Change": str, "Date of Most Recent Change (Data Change or Metadata Change)": str},
                                                                   copy=True)

    # SOCRATA UPSERT OF DATA
    # Due to problems with FME upsert process, or Socrata receiving data, we are implementing sodapy functionality
    # TODO: May want to extract this to a separate process at some point just for clarity
    records_dict_list = socrata_upsert_version.to_dict(orient="records")

    # Utility.upsert_to_socrata(client=DatasetSocrata.SOCRATA_CLIENT,
    #                           dataset_identifier=credentials_parser["SOCRATA"]["data_freshness_fourbyfour"],
    #                           zipper=records_dict_list)
    # print("\tUpserted to Socrata")

    # JSON Portion of the process. For a search portal that is under development.
    for dirname, dirs, files in os.walk(data_file_dir):
        # print(files)
        for file in files:
            file_path = os.path.join(dirname, file)
            exists = os.path.exists(file_path)
            print(file_path, exists)
            if file == agol_json_file_name and exists:
                with open(os.path.join(dirname, file), 'r') as agol_handler:
                    agol_json = json.loads(agol_handler.read())
            elif file == socrata_json_file_name and exists:
                with open(os.path.join(dirname, file), 'r') as socrata_handler:
                    socrata_json = json.loads(socrata_handler.read())
        break  # Just making sure

    # Add the two realms together for a unified output
    combined_json_contents = agol_json + socrata_json

    # Write the compiled output json file
    with open(combined_data_file_name_json, 'w') as compiled_handler:
        compiled_handler.write(json.dumps(combined_json_contents))


if __name__ == "__main__":
    main()
