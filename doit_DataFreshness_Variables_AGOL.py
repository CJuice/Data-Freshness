"""
File designated for process variables in order to centralize variables, and de-clutter main script.
"""
import datetime

# NOT DERIVED
arcgis_root_url = r"https://www.arcgis.com"
arcgis_item_url = "https://maryland.maps.arcgis.com/home/item.html?id={item_id}"
null_string = "NULL"
output_excel_file_path = r"Docs\AGOL_data_output.xlsx"
output_excel_sheetname = "The Data Nasty"
better_metadata_needed = "Better Metadata Needed."
all_map_layers = "All map layers from MD iMAP are in the process of being surveyed to determine this information."
metadata_missing = "Metadata on update frequency are missing. Dataset owner should provide this information to resolve this issue."
other_update_frequency = "Other Update Frequency - If frequency isn't included in list above, please describe it here."
update_frequency_missing = "Update frequency metadata are missing. Dataset owner should add metadata to resolve this issue."
update_frequency_unknown = "Update frequency metadata is Unknown. Dataset owner should add metadata to resolve this issue."
updated_enough_yes = "Yes"
updated_enough_no = "No"
whether_dataset = "Whether dataset is up to date cannot be calculated until the Department of Information Technology collects metadata on update frequency."

# DERIVED
arcgis_sharing_rest_url = f"{arcgis_root_url}/sharing/rest"
arcgis_data_catalog_url = f"{arcgis_sharing_rest_url}/search"
arcgis_metadata_url = "{arcgis_sharing_rest_url}/content/items/{item_id}/info/metadata/metadata.xml"
evaluation_difficult = f"{updated_enough_yes}. The data are updated as needed, which makes evaluation difficult. As an approximate measure, this dataset is evaluated as updated recently enough because it has been updated in the past month."
process_initiation_datetime = datetime.datetime.now()
number_of_seconds_in_a_day = 86400
