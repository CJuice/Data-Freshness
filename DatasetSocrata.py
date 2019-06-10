"""

"""
import os
import DataFreshness.Variables as var


class DatasetSocrata:
    """

    """


    def __init__(self, dataset_json: dict):
        """

        :param dataset_json:
        """
        self.access_level = None
        self.contact_point_dict = None
        self.dataset_json = dataset_json
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
        self.theme_list = None
        self.title = None
        self.type = None

    def assign_data_json_to_class_values(self):
        """

        :return:
        """
        self.access_level = self.dataset_json.get("accessLevel", None)
        self.contact_point_dict = self.dataset_json.get("contactPoint", None)
        self.description = self.dataset_json.get("description", None)
        self.distribution_list = self.dataset_json.get("distribution", None)
        self.identifier_url = self.dataset_json.get("identifier", None)
        self.issued = self.dataset_json.get("issued", None)
        self.keyword_list = self.dataset_json.get("keyword", None)
        self.landing_page = self.dataset_json.get("landingPage", None)
        self.modified = self.dataset_json.get("modified", None)
        self.publisher_dict = self.dataset_json.get("publisher", None)
        self.type = self.dataset_json.get("@type", None)
        self.theme_list = self.dataset_json.get("theme", None)
        self.title = self.dataset_json.get("title", None)
        return None

    def build_metadata_url(self):
        """

        :return:
        """
        self.metadata_url = f"{var.md_open_data_url}/api/views/{self.four_by_four}.json"
        return None

    def extract_four_by_four(self):
        """

        :return:
        """
        self.four_by_four = os.path.basename(self.landing_page)
        return None
