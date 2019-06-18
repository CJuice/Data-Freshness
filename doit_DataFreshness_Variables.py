"""
File designated for process variables in order to centralize variables, and de-clutter main script.
"""
import time
credentials_config_file_path = r"doit_DataFreshness_Credentials/doit_DataFreshness_Credentials.cfg"
process_initiation_datetime_in_seconds = float(round(time.time()))
number_of_seconds_in_a_day = 86400
md_open_data_domain = r"opendata.maryland.gov"
md_open_data_url = f"https://{md_open_data_domain}"
md_socrata_data_json_url = f"{md_open_data_url}/data.json"
md_socrata_profile_url = "{root}/profile/{user_four_by_four}"
other_update_frequency = "Other Update Frequency - If frequency isn't included in list above, please describe it here."
please_describe_below = "Other (Please Describe Below)"
# output_report_headers = ["Dataset Name", "Link", "Agency Performing Data Updates", "Owner", "Data Provided By",
#                          "Source URL", "User who Made Last Update", "Update Frequency",
#                          "Date of Most Recent Data Change", "Days Since Last Data Update",
#                          "Date of Most Recent View Change in Data or Metadata", "Updated Recently Enough",
#                          "Number of Rows", "Tags Keywords", "Column Names", "Missing Metadata Fields",
#                          "Portal", "Category"]
dataframe_to_header_mapping_for_output = {"Dataset Name": "title", "Link": "landing_page",
                                          "Agency Performing Data Updates": "state_agency_performing_data_updates",
                                          "Owner": "owner",
                                          "Data Provided By": "data_provided_by",
                                          "Source URL": "source_link",
                                          # "User who Made Last Update": "rows_updated_by",  # FIXME: Link is not public, can remove from report
                                          "Update Frequency": "update_frequency",
                                          "Date of Most Recent Data Change": "date_of_most_recent_data_change",
                                          "Days Since Last Data Update": "days_since_last_data_update",
                                          "Date of Most Recent View Change in Data or Metadata": "date_of_most_recent_view_change",
                                          "Days Since Last View Update": "days_since_last_view_update",
                                          "Updated Recently Enough": "updated_recently_enough",
                                          "Number of Rows": "number_of_rows_in_dataset",
                                          "Tags Keywords": "keyword_list", "Column Names": "column_names_string",
                                          "Missing Metadata Fields": "missing_metadata_fields", "Portal": "portal",
                                          "Category": "category_string"}

# TODO: dictionary of columns and their types for pandas.
# output_report_headers_column_types = {"Dataset Name": str, "Link": str, "Agency Performing Data Updates": str,
#                                       "Owner": str,
#                                       "Data Provided By": str, "Source URL": str, "User who Made Last Update": str,
#                                       "Update Frequency": str, "Date of Most Recent Data Change": "datetime",
#                                       "Days Since Last Data Update": int,
#                                       "Date of Most Recent View Change in Data or Metadata": "datetime",
#                                       "Updated Recently Enough": str, "Number of Rows": int, "Tags Keywords": str,
#                                       "Column Names": str, "Missing Metadata Fields": str, "Portal": str,
#                                       "Category": str}
output_excel_file_path = r"Docs\Socrata_data_output.xlsx"
output_excel_sheetname = "The Data Nasty"
updated_enough_affirmative = "Yes"
updated_enough_negative = "No"
better_metadata_needed = "Better Metadata Needed."
all_map_layers = "All map layers from MD iMAP are in the process of being surveyed to determine this information."
whether_dataset = "Whether dataset is up to date cannot be calculated until the Department of Information Technology collects metadata on update frequency."
update_frequency_missing = "Update frequency metadata are missing. Dataset owner should add metadata to resolve this issue."
metadata_missing = "Metadata on update frequency are missing. Dataset owner should provide this information to resolve this issue."
as_needed_needs_processing = "As Needed, requires processing"
static_cut_string = f"{updated_enough_affirmative}. The data are updated as needed, which makes evaluation difficult. As an approximate measure, this dataset is evaluated as updated recently enough because it has been updated in the past month."
socrata_updated_enough_dict = {"Monthly": 31, "Static Data": updated_enough_affirmative, "Every 10 Years": 3650,
                               "Annually": 365, "Quarterly": 91, "Continually": 31, "Weekly": 7, "Daily": 1,
                               "Triennially (Every Three Years)": 1095, "Biannually": 730,
                               "Semiannually (Twice per Year)": 183, "Static Cut": static_cut_string,
                               "As Needed": as_needed_needs_processing,
                               all_map_layers: f"{better_metadata_needed} {whether_dataset}",
                               "": f"{better_metadata_needed} {update_frequency_missing}",
                               }