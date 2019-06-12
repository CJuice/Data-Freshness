"""

"""
import os
import DataFreshness.doit_DataFreshness_Variables as var
from sodapy import Socrata
import time
import itertools


class DatasetSocrata:
    """

    """

    # Class attributes available to all instances
    SOCRATA_CLIENT = None
    LIMIT_MAX_AND_OFFSET = 10000
    SOCRATA_DATASET_TITLE_EXCLUSION_FILTERS = ("MD iMAP:", "Dataset Freshness", "Homepage Categories")


    # Methods
    def __init__(self):
        """

        Values in asset inventory json that were previously sourced from data.json have been ignored. The following
        listing captures the decisions made while building:

        KEY: d = data.json, a = asset inventory, m = metadata
        Asset Inventory Notes:
        dataset_link - landingPage
        u_id - four by four code extracted from landing page value (d)
        type - @type (d)
        name- title (d)
        description - description (d)
        last_update_date_data - publicationDate (m)
        category - theme (d)
        keywords - keyword (d)
        owner - contactPoint (d)
        contactemail - contactPoint (d)

        Metadata Notes:
        id - four by four code extracted from landing page value (d)
        name - title (d)
        attribution - data provided by (a)
        attributionLink - sourceLink (a)
        category - theme (d)
        createdAt - creation_date (a)
        description - description (d)
        downloadCount - downloads (a)
        iconUrl - Not stored, did not appear to be useful
        indexUpdatedAt - modified (d)
        licenseId - license (a)
        newBackend - Not stored, did not appear to be useful
        provenance - provenance (a)
        publicationDate - issued (d)
        publicationStage - publication_stage (a)

        :param dataset_json:
        """
        # DATA.JSON SOURCED VALUES
        self.access_level = None
        self.contact_point_dict = None
        self.description = None
        self.distribution_list = None
        self.four_by_four = None
        self.identifier_url = None
        self.keyword_list = None
        self.landing_page = None
        self.issued = None
        self.metadata_url = None
        self.modified = None
        self.publisher_dict = None
        self.resource_url = None
        self.theme_list = None
        self.title = None
        self.type = None

        # ASSET INVENTORY SOURCED VALUES
        self.creation_date = None
        self.data_provided_by = None
        self.date_metadata_written = None
        self.derived_view = None
        self.domain = None
        self.downloads = None
        self.jurisdiction = None
        self.last_update_date_data = None
        self.license = None
        self.owner_u_id = None
        self.place_keywords = None
        self.provenance = None
        self.public = None
        self.publication_stage = None
        self.source_link = None
        self.state_agency_performing_data_updates = None
        self.time_period_of_content = None
        self.update_frequency = None
        self.visits = None

        # METADATA SOURCE VALUES
        self.average_rating = None
        self.display_type = None
        self.hide_from_catalog = None
        self.hide_from_data_json = None
        self.number_of_comments = None
        self.oid = None
        self.publication_append_enabled = None
        self.publication_group = None
        self.row_class = None



    def assign_asset_inventory_json_to_class_values(self, asset_json):
        """

        :param asset_json:
        :return:
        """
        self.creation_date = asset_json.get("creation_date", None)
        self.data_provided_by = asset_json.get("data_provided_by", None)
        self.date_metadata_written = asset_json.get("date_metadata_written", None)
        self.derived_view = asset_json.get("derived_view", None)
        self.domain = asset_json.get("domain", None)
        self.downloads = asset_json.get("downloads", -9999)
        self.jurisdiction = asset_json.get("jurisdiction", None)
        self.last_update_date_data = asset_json.get("last_update_date_data", None)
        self.license = asset_json.get("license", None)
        self.owner_u_id = asset_json.get("owner_uid", None)
        self.place_keywords = asset_json.get("place_keywords", None)
        self.provenance = asset_json.get("provenance", None)
        self.public = asset_json.get("public", None)
        self.publication_stage = asset_json.get("publication_stage", None)
        self.source_link = asset_json.get("source_link", None)
        self.state_agency_performing_data_updates = asset_json.get("state_agency_performing_data_updates", None)
        self.time_period_of_content = asset_json.get("time_period_of_content", None)
        self.update_frequency = asset_json.get("update_frequency", None)
        self.visits = asset_json.get("visits", -9999)
        return None

    def assign_data_json_to_class_values(self, dataset_json: dict):
        """

        :return:
        """
        self.access_level = dataset_json.get("accessLevel", None)
        self.contact_point_dict = dataset_json.get("contactPoint", None)
        self.description = dataset_json.get("description", None)
        self.distribution_list = dataset_json.get("distribution", None)
        self.identifier_url = dataset_json.get("identifier", None)
        self.keyword_list = dataset_json.get("keyword", None)
        self.landing_page = dataset_json.get("landingPage", None)
        self.modified = dataset_json.get("modified", None)
        self.publisher_dict = dataset_json.get("publisher", None)
        self.type = dataset_json.get("@type", None)
        self.theme_list = dataset_json.get("theme", None)
        self.title = dataset_json.get("title", None)
        return None

    def assign_metadata_json_to_class_values(self, metadata_json: dict):
        self.average_rating = metadata_json.get("averageRating", None)
        self.display_type = metadata_json.get("displayType", None)
        self.hide_from_catalog = metadata_json.get("hideFromCatalog", None)
        self.hide_from_data_json = metadata_json.get("hideFromDataJson", None)
        self.number_of_comments = metadata_json.get("numberOfComments", None)
        self.oid = metadata_json.get("oid", None)
        self.publication_append_enabled = metadata_json.get("publicationAppendEnabled", None)
        self.publication_group = metadata_json.get("publicationGroup", None)
        self.row_class = metadata_json.get("rowClass", None)
        self. = metadata_json.get("", None)
        self. = metadata_json.get("", None)
        self. = metadata_json.get("", None)
        self. = metadata_json.get("", None)
        return

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

    # def cast_and_convert_class_attributes(self): # Going to do in pandas dataframe


    def extract_four_by_four(self):
        """

        :return:
        """
        self.four_by_four = os.path.basename(self.landing_page)
        return None

    @staticmethod
    def create_socrata_client(domain: str, app_token: str, username:str, password: str) -> Socrata:
        """
        Create and return a Socrata client for use.

        NOTE_1: It seems absolutely essential the the domain be a domain and not a url; 'https://opendata.maryland.gov'
            will not substitute for 'opendata.maryland.gov'.

        :param domain: domain for maryland open data portal.
        :param app_token: application token for throttling limitations
        :param username: account username
        :param password: password for account
        :return: Socrata connection client
        """

        return Socrata(domain=domain, app_token=app_token, username=username, password=password)

    @staticmethod
    def request_and_aggregate_all_socrata_records(client: Socrata, fourbyfour: str, limit_max_and_offset: int = LIMIT_MAX_AND_OFFSET) -> list:
        """

        :param client:
        :param fourbyfour:
        :param limit_max_and_offset:
        :return:
        """
        more_records_exist_than_response_limit_allows = True
        total_record_count = 0
        record_offset_value = 0
        master_list_of_dicts = []
        while more_records_exist_than_response_limit_allows:
            request_cycle_record_count = 0
            response = client.get(dataset_identifier=fourbyfour,
                                  content_type="json",
                                  limit=limit_max_and_offset,
                                  offset=record_offset_value)
            master_list_of_dicts.extend(response)

            number_of_records_returned = len(response)
            request_cycle_record_count += number_of_records_returned
            total_record_count += number_of_records_returned

            # Any cycle_record_count that equals the max limit indicates another request is needed
            if request_cycle_record_count == limit_max_and_offset:

                # Give Socrata servers small interval before requesting more
                time.sleep(0.2)
                record_offset_value = request_cycle_record_count + record_offset_value
            else:
                more_records_exist_than_response_limit_allows = False
        return master_list_of_dicts

    def passes_filter(self, gis_counter: itertools.count):

        if self.title is None:
            print(f"Unexpectedly encountered None value for self.title during passes_filter() call: {self.__dict__}")
            return False
        elif self.title.startswith("MD iMAP"):
            next(gis_counter)
            return False
        elif self.title.startswith("Dataset Freshness"):
            print("Dataset Freshness dataset encountered during passes_filter(). skipped.")
            print(f"\tTITLE: {self.title}")
            return False
        elif self.title.startswith("Homepage Categories"):
            # This was a filter used in the original design. Have not see any of these but preserving functionality.
            print("Homepage Categories title encountered during passes_filter(). Skipped")
            return False
        else:
            return True
