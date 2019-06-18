"""

"""
import datetime
import itertools
import os
import time
import DataFreshness.doit_DataFreshness_Variables as var
from sodapy import Socrata


class DatasetSocrata:
    """

    """

    # Class attributes available to all instances
    SOCRATA_CLIENT = None
    LIMIT_MAX_AND_OFFSET = 10000
    SOCRATA_DATASET_TITLE_EXCLUSION_FILTERS = ("MD iMAP:", "Dataset Freshness", "Homepage Categories")

    def __init__(self):
        """

        Values in asset inventory json that were previously sourced from data.json have been ignored. The following
        listing captures the decisions made while building:

        d = data.json, a = asset inventory, m = metadata

        DATA JSON NOTES:
        contactPoint - dict values covered elsewhere without need for extraction (owner (a), contactEmail (a))
        modified - viewLastModified (m) or indexUpdatedAt (m) (more detailed but these two are not identical)

        ASSET INVENTORY NOTES:
        category - theme (d)
        dataset_link - landingPage(d)
        description - description (d)
        keywords - keyword (d)
        name- title (d)
        type - @type (d)
        u_id - four by four code extracted from landing page value (d)

        METADATA NOTES:
        attribution - data provided by (a)
        attributionLink - sourceLink (a)
        category - theme (d)
        createdAt - creation_date (a)
        description - description (d)
        disabledFeatureFlag - Not stored, did not appear to be useful
        downloadCount - downloads (a)
        flags - Not stored, did not appear to be useful
        grants - Not stored, did not appear to be useful
        iconUrl - Not stored, did not appear to be useful
        id - four by four code extracted from landing page value (d)
        license - Appears to be covered by license (a) or even licenseId (m)
        licenseId - license (a)
        metadata - Most if not all info is already captured. But update frequency parameter essential.
            Jurisdiction - jurisdiction (a)
            Agency - state_agency_performing_updates (a)
            Time Period
                Update Frequency - update_frequency (a)
                Time Period of Content - time_period_of_content (a)
                Date Metadata Written - date_metadata_written (a)
                'Other Update Frequency - If frequency isn't included in list above, please describe it here.' - UNIQUE!
            Place Keywords - place_keywords (a)
        name - title (d)
        newBackend - Not stored, did not appear to be useful
        owner - dict values covered elsewhere without need for extraction (owner_u_id (a), owner (a))
        provenance - provenance (a)
        publicationStage - publication_stage (a)
        query - Not stored, did not appear to be useful
        rights - Not stored, did not appear to be useful
        tableAuthor - dict values covered elsewhere without need for extraction (owner_u_id (a), owner (a))
        tags - keyword (d)
        viewCount - visits (a)
        viewType - Not stored, did not appear to be useful

        :param dataset_json:
        """
        # FIXME: Resolve differences between, and need of or value of, issued (d) vs last_update_date_data (a) publicationDate (m) vs rowsUpdatedAt (m) vs indexUpdatedAt (m)
        # NOTES: issued is most generic and only accurate to the day.
        self.portal = "Socrata"

        # DATA.JSON SOURCED VALUES
        self.access_level = None
        self.description = None
        self.distribution_list = None
        self.four_by_four = None
        self.identifier_url = None
        self.keyword_list = None
        self.landing_page = None
        self.issued = None
        self.metadata_url = None
        self.publisher_dict = None
        self.resource_url = None
        self.theme_list = None
        self.title = None
        self.type = None

        # ASSET INVENTORY SOURCED VALUES
        self.contact_email = None
        self.creation_date = None
        self.data_provided_by = None
        self.date_metadata_written = None
        self.derived_view = None
        self.domain = None
        self.downloads = None
        self.jurisdiction = None
        self.last_update_date_data = None
        self.license = None
        self.owner = None
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
        self.approvals = None
        self.average_rating = None
        self.columns = None
        self.display_type = None
        self.hide_from_catalog = None
        self.hide_from_data_json = None
        self.index_updated_at = None
        self.number_of_comments = None
        self.oid = None
        self.other_update_frequency = None
        self.publication_append_enabled = None
        self.publication_date = None
        self.publication_group = None
        self.row_class = None
        self.rows_updated_at = None
        self.rows_updated_by = None
        self.table_id = None
        self.total_times_rated = None
        self.view_last_modified = None

        # DERIVED VALUES
        self.category_string = None
        self.column_names_string = None
        self.date_of_most_recent_data_change = None
        self.date_of_most_recent_view_change = None
        self.days_since_last_data_update = None
        self.days_since_last_view_update = None
        self.missing_metadata_fields = None  # FIXME: Hardcoded until develop function. just using so can output dataframe now
        self.number_of_rows_in_dataset = None
        self.updated_recently_enough = True  # FIXME: Hardcoded until develop function. just using so can output dataframe now

    def assemble_category_output_string(self):
        """
        """
        try:
            self.category_string = ", ".join(self.theme_list) if self.theme_list is not None else None
        except TypeError as te:
            print(f"TypeError in assemble_category_output_string(): {type(self.theme_list)}, {self.theme_list}, {te}")
            self.category_string = self.theme_list

    def assemble_column_names_output_string(self):
        """

        :return:
        """
        self.column_names_string = ", ".join([column_dict.get("name", None) for column_dict in self.columns])

    def assign_asset_inventory_json_to_class_values(self, asset_json):
        """

        :param asset_json:
        :return:
        """
        self.contact_email = asset_json.get("contactemail", None)
        self.creation_date = asset_json.get("creation_date", None)
        self.data_provided_by = asset_json.get("data_provided_by", None)
        self.date_metadata_written = asset_json.get("date_metadata_written", None)
        self.derived_view = asset_json.get("derived_view", None)
        self.domain = asset_json.get("domain", None)
        self.downloads = asset_json.get("downloads", -9999)
        self.jurisdiction = asset_json.get("jurisdiction", None)
        self.last_update_date_data = asset_json.get("last_update_date_data", None)
        self.license = asset_json.get("license", None)
        self.owner = asset_json.get("owner", None)
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

        :param dataset_json:
        :return:
        """
        self.access_level = dataset_json.get("accessLevel", None)
        self.description = dataset_json.get("description", None)
        self.distribution_list = dataset_json.get("distribution", None)
        self.identifier_url = dataset_json.get("identifier", None)
        self.issued = dataset_json.get("issued", None)
        self.keyword_list = dataset_json.get("keyword", None)
        self.landing_page = dataset_json.get("landingPage", None)
        self.publisher_dict = dataset_json.get("publisher", None)
        self.type = dataset_json.get("@type", None)
        self.theme_list = dataset_json.get("theme", None)
        self.title = dataset_json.get("title", None)
        return None

    def assign_metadata_json_to_class_values(self, metadata_json: dict):
        """

        :param metadata_json:
        :return:
        """
        self.approvals = metadata_json.get("approvals", None)
        self.average_rating = metadata_json.get("averageRating", None)
        self.columns = metadata_json.get("columns", None)
        self.display_type = metadata_json.get("displayType", None)
        self.hide_from_catalog = metadata_json.get("hideFromCatalog", None)
        self.hide_from_data_json = metadata_json.get("hideFromDataJson", None)
        self.number_of_comments = metadata_json.get("numberOfComments", None)
        self.index_updated_at = metadata_json.get("indexUpdatedAt", None)
        self.oid = metadata_json.get("oid", None)
        self.other_update_frequency = metadata_json.get("Time Period", {}).get(var.other_update_frequency, None)
        self.publication_append_enabled = metadata_json.get("publicationAppendEnabled", None)
        self.publication_date = metadata_json.get("publicationDate", None)
        self.publication_group = metadata_json.get("publicationGroup", None)
        self.row_class = metadata_json.get("rowClass", None)
        self.rows_updated_at = metadata_json.get("rowsUpdatedAt", None)
        self.rows_updated_by = metadata_json.get("rowsUpdatedBy", None)
        self.table_id = metadata_json.get("tableId", None)
        self.total_times_rated = metadata_json.get("totalTimesRated", None)
        self.view_last_modified = metadata_json.get("viewLastModified", None)
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

    def calculate_date_of_most_recent_data_change(self):
        """

        The choices made in this function originated in the original Date Freshness process.
        :return:
        """
        if self.rows_updated_at is None:
            self.rows_updated_at = self.publication_date
        self.date_of_most_recent_data_change = datetime.datetime.fromtimestamp(self.rows_updated_at)

    def calculate_date_of_most_recent_view_change(self):
        """

        The choices made in this function originated in the original Date Freshness process.
        :return:
        """
        if self.view_last_modified is None:
            self.view_last_modified = self.publication_date
        self.date_of_most_recent_view_change = datetime.datetime.fromtimestamp(self.rows_updated_at)

    def calculate_days_since_last_data_update(self):
        """

        :return:
        """
        self.days_since_last_data_update = int(round(
                (var.process_initiation_datetime_in_seconds - self.rows_updated_at) / var.number_of_seconds_in_a_day))

    def calculate_days_since_last_view_change(self):
        """

        :return:
        """
        self.days_since_last_view_update = int(round(
            (var.process_initiation_datetime_in_seconds - self.view_last_modified) / var.number_of_seconds_in_a_day))

    def calculate_number_of_rows_in_dataset(self):
        """

        NOTE: The 'non_null' key is missing from some datasets
        :return:
        """
        first_column_dict = self.columns[0] if 0 < len(self.columns) else {}
        cached_contents_dict = first_column_dict.get("cachedContents", {})
        try:
            self.number_of_rows_in_dataset = sum([int(cached_contents_dict.get("non_null")),
                                              int(cached_contents_dict.get("null"))]) if 0 < len(cached_contents_dict) else -9999
        except TypeError as te:
            print(f"TypeError in calculate_number_of_rows_in_dataset( ({self.four_by_four}) non_null={cached_contents_dict.get('non_null')}, null={cached_contents_dict.get('null')} {te}")
            self.number_of_rows_in_dataset = -9999
        # TODO: For datasets that don't supply a non_null count then may need to make requests to actual dataset and count
        #   the number of records. This is more efficient than doing so for every single dataet. Only making costly requests for subset of all datasets.

    def extract_four_by_four(self):
        """

        :return:
        """
        self.four_by_four = os.path.basename(self.landing_page)
        return None

    # def cast_and_convert_class_attributes(self): # Going to do in pandas dataframe

    def passes_filter_data_json(self, gis_counter: itertools.count, dataset_freshness_counter: itertools.count):
        """

        :param gis_counter:
        :param dataset_freshness_counter:
        :return:
        """

        if self.title is None:
            print(f"Unexpectedly encountered None value for self.title during passes_filter() call: {self.__dict__}")
            return False
        elif self.title.startswith("MD iMAP"):
            next(gis_counter)
            return False
        elif self.title.startswith("Dataset Freshness"):
            print("Dataset Freshness dataset encountered during passes_filter(). skipped.")
            print(f"\tTITLE: {self.title}")
            next(dataset_freshness_counter)
            return False
        elif self.title.startswith("Homepage Categories"):
            # This was a filter used in the original design. Have not see any of these but preserving functionality.
            print("Homepage Categories title encountered during passes_filter(). Skipped")
            return False
        else:
            return True

    def passes_filter_metadata_json(self, gis_counter: itertools.count):
        """

        :param gis_counter:
        :return:
        """

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

    def process_update_frequency(self):
        """

        :return:
        """
        if self.update_frequency is None:
            self.update_frequency = "Error"
        elif self.update_frequency == var.please_describe_below and self.other_update_frequency is not None:
            self.update_frequency = self.other_update_frequency
        else:
            pass
        return

    # TODO: After assessing output values determine if need to process update frequency further
    # def process_update_frequency(self):
    #     """
    #
    #     :return:
    #     """
    #     if self.update_frequency is None:
    #         self.update_frequency =

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
    def request_and_aggregate_all_socrata_records(client: Socrata, fourbyfour: str,
                                                  limit_max_and_offset: int = LIMIT_MAX_AND_OFFSET) -> list:
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
