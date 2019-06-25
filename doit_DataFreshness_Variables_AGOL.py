"""
File designated for process variables in order to centralize variables, and de-clutter main script.
"""
import time

# NOT DERIVED
arcgis_root_url = r"https://www.arcgis.com"
arcgis_item_url = "https://maryland.maps.arcgis.com/home/item.html?id={item_id}"
# DERIVED
arcgis_sharing_rest_url = f"{arcgis_root_url}/sharing/rest"
arcgis_data_catalog_url = f"{arcgis_sharing_rest_url}/search"
arcgis_metadata_url = "{arcgis_sharing_rest_url}/content/items/{item_id}/info/metadata/metadata.xml"
null_string = "NULL"
output_excel_file_path = r"Docs\AGOL_data_output.xlsx"
output_excel_sheetname = "The Data Nasty"
