"""

"""
import os
import DataFreshness.doit_DataFreshness_Variables as var


class DatasetSocrata:
    """

    """

    def __init__(self):
        """

        :param dataset_json:
        """
        self.access_level = None
        self.asset_inventory_url = None
        self.contact_point_dict = None
        # self.dataset_json = dataset_json
        self.description = None
        self.distribution_list = None
        self.four_by_four = None
        self.identifier_url = None
        self.issued = None
        self.keyword_list = None
        self.landing_page = None
        self.metadata_url = None
        self.modified = None
        self.publisher_dict = None
        self.resource_url = None
        self.theme_list = None
        self.title = None
        self.type = None

    def assign_data_json_to_class_values(self, dataset_json: dict):
        """

        :return:
        """
        self.access_level = dataset_json.get("accessLevel", None)
        self.contact_point_dict = dataset_json.get("contactPoint", None)
        self.description = dataset_json.get("description", None)
        self.distribution_list = dataset_json.get("distribution", None)
        self.identifier_url = dataset_json.get("identifier", None)
        self.issued = dataset_json.get("issued", None)
        self.keyword_list = dataset_json.get("keyword", None)
        self.landing_page = dataset_json.get("landingPage", None)
        self.modified = dataset_json.get("modified", None)
        self.publisher_dict = dataset_json.get("publisher", None)
        self.type = dataset_json.get("@type", None)
        self.theme_list = dataset_json.get("theme", None)
        self.title = dataset_json.get("title", None)
        return None

    def build_metadata_url(self):
        """

        :return:
        """
        self.metadata_url = f"{var.md_open_data_url}/api/views/{self.four_by_four}.json"
        return None

    def build_resource_url(self):
        """

        :return:
        """
        self.resource_url = f"{var.md_open_data_url}/resource/{self.four_by_four}.json"
        return None

    def build_asset_inventory_url(self, fourbyfour):
        """

        :return:
        """
        self.asset_inventory_url = f"{var.md_open_data_url}/resource/{fourbyfour}.json?u_id={cur_identifier}&$$app_token={socrata_app_token}"
        return None

    def extract_four_by_four(self):
        """

        :return:
        """
        self.four_by_four = os.path.basename(self.landing_page)
        return None
