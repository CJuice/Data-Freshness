"""
File designated for process variables in order to centralize variables, and de-clutter main script.
Author: CJuice
Date: 20190702
Revisions:
    20190708, CJuice: Altered the fields output to excel. Including the unique id in output excel.
    20190717, CJuice: Changed header mappings to match Socrata field names or wouldn't upsert values properly
    20201207, CJuice: Revised field names after Socrata Asset Inventory api was revised
"""
import datetime
import os
import time

# NOT DERIVED
_root_file_path = os.path.dirname(__file__)
all_map_layers = "All map layers from MD iMAP are in the process of being surveyed to determine this information."
better_metadata_needed = "Better Metadata Needed."
dataframe_to_header_mapping_for_excel_output = {"Unique Identifier": "four_by_four",
                                                "Dataset Name": "title",
                                                "Link": "landing_page",
                                                "Agency Performing Data Updates": "agency_stateagencyperformingdataupdates",
                                                "Owner": "owner",
                                                "Data Provided By": "attribution",
                                                "Source URL": "attribution_link",
                                                "Update Frequency": "timeperiod_updatefrequency",
                                                "Date of Most Recent Data Change": "date_of_most_recent_data_change",
                                                "Days Since Most Recent Data Change": "days_since_most_recent_data_change",
                                                "Date of Most Recent Change (Data Change or Metadata Change)": "date_of_most_recent_view_change_data_or_metadata",
                                                # "Days Since Last View Update": "days_since_last_view_update",
                                                "Updated Recently Enough?": "updated_recently_enough",
                                                "Number of Rows": "number_of_rows_in_dataset",
                                                "Tags / Keywords": "keyword_tags_string",
                                                "Column Names": "column_names_string",
                                                "Missing Metadata Fields": "missing_metadata_fields",
                                                "Portal": "portal",
                                                "Category": "category_string"}
expected_socrata_asset_inventory_json_keys_dict = {'api_endpoint': 'API Endpoint',
                                                   'category': 'Category',
                                                   'contact_email': 'Contact Email',
                                                   'creation_date': 'Creation Date',
                                                   'attribution': 'Data Provided By',
                                                   'dataset_link': 'Dataset Link',
                                                   'date_metadata_written': 'Date Metadata Written',
                                                   'derived_view': 'Derived View',
                                                   'description': 'Description',
                                                   'domain': 'Domain',
                                                   'downloads': 'Downloads',
                                                   'jurisdiction_jurisdiction': 'Jurisdiction',
                                                   'keywords': 'Tags/Keywords',
                                                   'last_data_updated_date': 'Data Last Update Date',
                                                   'license': 'License',
                                                   'name': 'Title/Name',
                                                   'owner': 'Owner',
                                                   'owner_uid': 'Owner ID',
                                                   'gisdownload_placekeywords': 'Place Keywords',
                                                   'provenance': 'Provenance',
                                                   'audience': 'Public',
                                                   'publication_stage': 'Publication Stage',
                                                   'attribution_link': 'Source Link',
                                                   'agency_stateagencyperformingdataupdates': 'State Agency Performing Updates',
                                                   'timeperiod_timeperiodofcontent': 'Time Period of Content',
                                                   'type': 'Type',
                                                   'u_id': 'Dataset ID',
                                                   'timeperiod_updatefrequency': 'Update Frequency',
                                                   'visits': 'Visits'}
json_output_columns_list = ["category_string", "description", "four_by_four", "date_of_most_recent_data_change", "portal", "attribution", "keyword_tags_string", "title", "type_", "landing_page"]
md_open_data_domain = r"opendata.maryland.gov"
md_socrata_profile_url = "{root}/profile/{user_four_by_four}"
metadata_missing = "Metadata on update frequency are missing. Dataset owner should provide this information to resolve this issue."
null_string = "NULL"
number_of_seconds_in_a_day = 86400
other_update_frequency = "Other Update Frequency - If frequency isn't included in list above, please describe it here."
output_excel_sheetname = "The Data Nasty"
please_describe_below = "Other (Please Describe Below)"
process_initiation_datetime_in_seconds = float(round(time.time()))
update_frequency_missing = "Update frequency metadata are missing. Dataset owner should add metadata to resolve this issue."
updated_enough_no = "No"
updated_enough_yes = "Yes"
whether_dataset = "Whether dataset is up to date cannot be calculated until the Department of Information Technology collects metadata on update frequency."

# DERIVED
credentials_config_file_path = f"{_root_file_path}/doit_DataFreshness_Credentials/doit_DataFreshness_Credentials.cfg"

evaluation_difficult = f"{updated_enough_yes}. The data are updated as needed, which makes evaluation difficult. As an approximate measure, this dataset is evaluated as updated recently enough because it has been updated in the past month."
md_open_data_url = f"https://{md_open_data_domain}"
md_socrata_data_json_url = f"{md_open_data_url}/data.json"
output_excel_file_path_data_freshness_SOCRATA = f"{_root_file_path}/DataFreshnessOutputs/SOCRATA_data_freshness.xlsx"
output_excel_file_path_full_dataframe = r"{_root_file_path}/Docs/{date}SOCRATA_data_output.xlsx".format(_root_file_path=_root_file_path, date=datetime.datetime.now().strftime('%Y%m%d'))
output_json_file_path_data_freshness_SOCRATA = f"{_root_file_path}/DataFreshnessOutputs/SOCRATA_data_freshness.json"

