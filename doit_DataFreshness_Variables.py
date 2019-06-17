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
# output_report_headers = ["Dataset Name", "Link", "Agency Performing Data Updates", "Owner", "Data Provided By",
#                          "Source URL", "User who Made Last Update", "Update Frequency",
#                          "Date of Most Recent Data Change", "Days Since Last Data Update",
#                          "Date of Most Recent View Change in Data or Metadata", "Updated Recently Enough",
#                          "Number of Rows", "Tags Keywords", "Column Names", "Missing Metadata Fields",
#                          "Portal", "Category"]
dataframe_to_header_mapping_for_output = {"Dataset Name": "title", "Link": "landing_page",
                                          "Agency Performing Data Updates": "state_agency_performing_data_updates",
                                          "Owner": "owner",
                                          # "Data Provided By": "data_provided_by",  # FIXME: Link is not public, can remove from report
                                          "Source URL": "source_link", "User who Made Last Update": "rows_updated_by",
                                          "Update Frequency": "update_frequency",
                                          "Date of Most Recent Data Change": "date_of_most_recent_data_change",
                                          "Days Since Last Data Update": "days_since_last_data_update",
                                          "Date of Most Recent View Change in Data or Metadata": "date_of_most_recent_view_change",
                                          "Updated Recently Enough": "updated_recently_enough",
                                          "Number of Rows": "number_of_rows_in_dataset",
                                          "Tags Keywords": "keyword_list", "Column Names": "column_names_list",
                                          "Missing Metadata Fields": "missing_metadata_fields", "Portal": "portal",
                                          "Category": "theme"}

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
