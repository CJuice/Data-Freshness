"""

"""
class DatasetSocrata:
    """

    """

    def __init__(self, dataset_json: dict):
        """

        :param dataset_json:
        """
        self.dataset_json = dataset_json
        self.access_level = None
        self.landing_page = None
        self.issued = None
        self.type = None
        self.modified = None
        self.keyword_list = None
        self.contact_point_dict = None
        self.publisher_dict = None
        self.identifier_url = None
        self.description = None
        self.title = None
        self.distribution_list = None
        self.theme_list = None

    def assign_data_json_to_class_values(self):
        """

        :return:
        """
        self.access_level = self.dataset_json.get("accessLevel", None)
        self.landing_page = self.dataset_json.get("landingPage", None)
        self.issued = self.dataset_json.get("issued", None)
        self.type = self.dataset_json.get("@type", None)
        self.modified = self.dataset_json.get("modified", None)
        self.keyword_list = self.dataset_json.get("keyword", None)
        self.contact_point_dict = self.dataset_json.get("contactPoint", None)
        self.publisher_dict = self.dataset_json.get("publisher", None)
        self.identifier_url = self.dataset_json.get("identifier", None)
        self.description = self.dataset_json.get("description", None)
        self.title = self.dataset_json.get("title", None)
        self.distribution_list = self.dataset_json.get("distribution", None)
        self.theme_list = self.dataset_json.get("theme", None)
        return
