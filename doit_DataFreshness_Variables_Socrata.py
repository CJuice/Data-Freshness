"""
File designated for process variables in order to centralize variables, and de-clutter main script.
Author: CJuice
Date: 20190702
Modifications:

"""
import time
import datetime

# NOT DERIVED
all_map_layers = "All map layers from MD iMAP are in the process of being surveyed to determine this information."
better_metadata_needed = "Better Metadata Needed."
credentials_config_file_path = r"doit_DataFreshness_Credentials/doit_DataFreshness_Credentials.cfg"
dataframe_to_header_mapping_for_excel_output = {"Dataset Name": "title",
                                                "Link": "landing_page",
                                                "Agency Performing Data Updates": "state_agency_performing_data_updates",
                                                "Owner": "owner",
                                                "Data Provided By": "data_provided_by",
                                                "Source URL": "source_link",
                                                "Update Frequency": "update_frequency",
                                                "Date of Most Recent Data Change": "date_of_most_recent_data_change",
                                                "Days Since Last Data Update": "days_since_last_data_update",
                                                "Date of Most Recent View Change in Data or Metadata": "date_of_most_recent_view_change",
                                                "Days Since Last View Update": "days_since_last_view_update",
                                                "Updated Recently Enough": "updated_recently_enough",
                                                "Number of Rows": "number_of_rows_in_dataset",
                                                "Tags Keywords": "keyword_tags_string",
                                                "Column Names": "column_names_string",
                                                "Missing Metadata Fields": "missing_metadata_fields",
                                                "Portal": "portal",
                                                "Category": "category_string"}
expected_socrata_asset_inventory_json_keys_dict = {'api_endpoint': 'API Endpoint',
                                                   'category': 'Category',
                                                   'contactemail': 'Contact Email',
                                                   'creation_date': 'Creation Date',
                                                   'data_provided_by': 'Data Provided By',
                                                   'dataset_link': 'Dataset Link',
                                                   'date_metadata_written': 'Date Metadata Written',
                                                   'derived_view': 'Derived View',
                                                   'description': 'Description',
                                                   'domain': 'Domain',
                                                   'downloads': 'Downloads',
                                                   'jurisdiction': 'Jurisdiction',
                                                   'keywords': 'Tags/Keywords',
                                                   'last_update_date_data': 'Data Last Update Date',
                                                   'license': 'License',
                                                   'name': 'Title/Name',
                                                   'owner': 'Owner',
                                                   'owner_uid': 'Owner ID',
                                                   'place_keywords': 'Place Keywords',
                                                   'provenance': 'Provenance',
                                                   'public': 'Public',
                                                   'publication_stage': 'Publication Stage',
                                                   'source_link': 'Source Link',
                                                   'state_agency_performing_data_updates': 'State Agency Performing Updates',
                                                   'time_period_of_content': 'Time Period of Content',
                                                   'type': 'Type',
                                                   'u_id': 'Dataset ID',
                                                   'update_frequency': 'Update Frequency',
                                                   'visits': 'Visits'}
json_output_columns_list = ["category_string", "description", "four_by_four", "date_of_most_recent_data_change", "portal", "data_provided_by", "keyword_tags_string", "title", "type_", "landing_page"]
md_open_data_domain = r"opendata.maryland.gov"
md_socrata_profile_url = "{root}/profile/{user_four_by_four}"
metadata_missing = "Metadata on update frequency are missing. Dataset owner should provide this information to resolve this issue."
null_string = "NULL"
number_of_seconds_in_a_day = 86400
other_update_frequency = "Other Update Frequency - If frequency isn't included in list above, please describe it here."
output_excel_file_path_data_freshness_SOCRATA = r"Docs\DataFreshnessOutputs\SOCRATA_data_freshness.xlsx"
output_excel_sheetname = "The Data Nasty"
output_json_file_path_data_freshness_SOCRATA = r"Docs\DataFreshnessOutputs\SOCRATA_data_freshness.json"
please_describe_below = "Other (Please Describe Below)"
process_initiation_datetime_in_seconds = float(round(time.time()))
update_frequency_missing = "Update frequency metadata are missing. Dataset owner should add metadata to resolve this issue."
updated_enough_no = "No"
updated_enough_yes = "Yes"
whether_dataset = "Whether dataset is up to date cannot be calculated until the Department of Information Technology collects metadata on update frequency."

# DERIVED
evaluation_difficult = f"{updated_enough_yes}. The data are updated as needed, which makes evaluation difficult. As an approximate measure, this dataset is evaluated as updated recently enough because it has been updated in the past month."
md_open_data_url = f"https://{md_open_data_domain}"
md_socrata_data_json_url = f"{md_open_data_url}/data.json"
output_excel_file_path_full_dataframe = r"Docs\{date}SOCRATA_data_output.xlsx".format(date=datetime.datetime.now().strftime('%Y%m%d'))

