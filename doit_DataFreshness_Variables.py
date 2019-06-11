"""
File designated for process variables in order to centralize variables, and de-clutter main script.
"""

credentials_config_file_path = r"doit_DataFreshness_Credentials/doit_DataFreshness_Credentials.cfg"

md_open_data_domain = r"opendata.maryland.gov"
md_open_data_url = fr"https://{md_open_data_domain}"
md_socrata_data_json_url = f"{md_open_data_url}/data.json"
output_report_columns = ["Dataset Name", "Link", "Agency Performing Data Updates", "Owner", "Data Provided By",
                         "Source URL", "User who Made Last Update", "Update Frequency",
                         "Date of Most Recent Data Change", "Days Since Last Data Update",
                         "Date of Most Recent View Change in Data or Metadata", "Updated Recently Enough",
                         "Number of Rows", "Tags Keywords", "Column Names", "Missing Metadata Fields",
                         "Portal", "Category"]

# TODO: dictionary of columns and their types for pandas.
output_report_columns_types = {"Dataset Name": str, "Link": str, "Agency Performing Data Updates": str, "Owner": str,
                               "Data Provided By": str, "Source URL": str, "User who Made Last Update": str,
                               "Update Frequency": str, "Date of Most Recent Data Change": "datetime",
                               "Days Since Last Data Update": int,
                               "Date of Most Recent View Change in Data or Metadata": "datetime",
                               "Updated Recently Enough": str, "Number of Rows": int, "Tags Keywords": str,
                               "Column Names": str, "Missing Metadata Fields": str, "Portal": str, "Category": str}
