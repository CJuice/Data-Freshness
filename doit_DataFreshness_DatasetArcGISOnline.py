"""

"""
import DataFreshness.doit_DataFreshness_Variables as var


class DatasetArcGISOnline:
    """

    """
    OWNER = 'owner:mdimapdatacatalog'
    RECORD_LIMIT = '100'
    SORT_FIELD = 'title'

    def __init__(self):
        self.arcgis_catalog_url = None


    def build_argis_catalog_url(self):
        self.arcgis_catalog_url = f"{var.arcgis_root_url}/sharing/rest/search"

    def build_arcgis_request_params(self, start_num: int = None) -> dict:
        return {
            'q': DatasetArcGISOnline.OWNER,
            'num': DatasetArcGISOnline.RECORD_LIMIT,
            'start': start_num,
            'sortField': DatasetArcGISOnline.SORT_FIELD,
            'f': 'json'
        }
