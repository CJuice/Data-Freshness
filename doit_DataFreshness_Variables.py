"""
File designated for process variables in order to centralize variables, and de-clutter main script.
"""
import time

# NOT DERIVED
all_map_layers = "All map layers from MD iMAP are in the process of being surveyed to determine this information."
arcgis_root_url = r"https://www.arcgis.com"
better_metadata_needed = "Better Metadata Needed."
credentials_config_file_path = r"doit_DataFreshness_Credentials/doit_DataFreshness_Credentials.cfg"
dataframe_to_header_mapping_for_output = {"Dataset Name": "title", "Link": "landing_page",
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
                                          "Tags Keywords": "keyword_tags_string", "Column Names": "column_names_string",
                                          "Missing Metadata Fields": "missing_metadata_fields", "Portal": "portal",
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
md_open_data_domain = r"opendata.maryland.gov"
md_socrata_profile_url = "{root}/profile/{user_four_by_four}"
metadata_missing = "Metadata on update frequency are missing. Dataset owner should provide this information to resolve this issue."
null_string = "NULL"
number_of_seconds_in_a_day = 86400
other_update_frequency = "Other Update Frequency - If frequency isn't included in list above, please describe it here."
output_excel_file_path = r"Docs\Socrata_data_output.xlsx"
output_excel_sheetname = "The Data Nasty"
please_describe_below = "Other (Please Describe Below)"
process_initiation_datetime_in_seconds = float(round(time.time()))
update_frequency_missing = "Update frequency metadata are missing. Dataset owner should add metadata to resolve this issue."
updated_enough_yes = "Yes"
updated_enough_no = "No"
whether_dataset = "Whether dataset is up to date cannot be calculated until the Department of Information Technology collects metadata on update frequency."

# DERIVED
arcgis_data_catalog_url = f"{arcgis_root_url}/sharing/rest/search"
evaluation_difficult = f"{updated_enough_yes}. The data are updated as needed, which makes evaluation difficult. As an approximate measure, this dataset is evaluated as updated recently enough because it has been updated in the past month."
md_open_data_url = f"https://{md_open_data_domain}"
md_socrata_data_json_url = f"{md_open_data_url}/data.json"

# TODO: dictionary of columns and their types for pandas. Does this matter?
# output_report_headers_column_types = {"Dataset Name": str, "Link": str, "Agency Performing Data Updates": str,
#                                       "Owner": str,
#                                       "Data Provided By": str, "Source URL": str, "User who Made Last Update": str,
#                                       "Update Frequency": str, "Date of Most Recent Data Change": "datetime",
#                                       "Days Since Last Data Update": int,
#                                       "Date of Most Recent View Change in Data or Metadata": "datetime",
#                                       "Updated Recently Enough": str, "Number of Rows": int, "Tags Keywords": str,
#                                       "Column Names": str, "Missing Metadata Fields": str, "Portal": str,
#                                       "Category": str}
