"""
Contains a single class for socrata dataset representation per MD DoIT needs.
Author: CJuice
Date: 20190702
Revision: 20190717, CJuice - Added Socrata upsert functionality to push the data directly to the data freshness dataset
    20201207, CJuice: Revised functionality, and attribute names after Socrata Asset Inventory api was revised
"""
import datetime
import itertools
import os
import time
import DataFreshness.doit_DataFreshness_Variables_Socrata as var
from sodapy import Socrata


class DatasetSocrata:
    """
    Class for storing information on a socrata datasets and for processing values for output needs.
    The limit max and offset can be changed. A 1000 is the default set by socrata.
    The client is used to access resources that are protected.
    The title exclusion filters are for focusing the processing and excluding items not of interest.
    An object is instantiated to None for all attributes and then the values are assigned after extraction from json or
        after processing.
    Attributes are organized by their original source or into derived values. Three resources are consulted and these
    are the data.json, the asset inventory, and the metadata json provided by Socrata. The values in the response json
    are extracted, assigned, and stored as attributes but the original json is not saved. The attributes have been
    organized alphabetically within their source groups. The derived values group are attributes that are derived from
    processing raw values from the json and involve decision making or conversions.
    """

    # Class attributes available to all instances
    LIMIT_MAX_AND_OFFSET = 10000
    SOCRATA_CLIENT = None
    SOCRATA_DATASET_TITLE_EXCLUSION_FILTERS = ("MD iMAP:", "Dataset Freshness", "Homepage Categories")

    def __init__(self):
        """
        Instantiate the default dataset object in preparation for value assignments.

        Values in asset inventory json (for example) that were previously sourced from data.json have been ignored.
         The following listing captures the decisions made while building this process.

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
        u_id - four by four code extracted from landing page value (d) #

        METADATA NOTES:
        attribution - attribution (a)
        attributionLink - attribution_link (a)
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
            Jurisdiction - jurisdiction_jurisdiction (a)
            Agency - agency_stateagencyperformingdataupdates (a)
            Time Period
                Update Frequency - timeperiod_updatefrequency (a)
                Time Period of Content - timeperiod_timeperiodofcontent (a)
                Date Metadata Written - date_metadata_written (a)
                'Other Update Frequency - If frequency isn't included in list above, please describe it here.' - UNIQUE!
            Place Keywords - gisdownload_placekeywords (a)
        name - title (d)
        newBackend - Not stored, did not appear to be useful
        owner - dict values covered elsewhere without need for extraction (owner_uid (a), owner (a))
        provenance - provenance (a)
        publicationStage - publication_stage (a)
        query - Not stored, did not appear to be useful
        rights - Not stored, did not appear to be useful
        tableAuthor - dict values covered elsewhere without need for extraction (owner_uid (a), owner (a))
        tags - keyword (d)
        viewCount - visits (a)
        viewType - Not stored, did not appear to be useful
        """

        # TODO: Resolve differences between, and need of or value of, issued (d) vs last_update_date_data (a) publicationDate (m) vs rowsUpdatedAt (m) vs indexUpdatedAt (m)
        # NOTES: issued is most generic and only accurate to the day.

        self.portal = "Socrata"  # Would make a constant but mapping to pandas dataframe field becomes more cumbersome.

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
        self.type_ = None

        # ASSET INVENTORY SOURCED VALUES
        self.agency_stateagencyperformingdataupdates = None
        self.attribution = None
        self.attribution_link = None
        self.audience = None
        self.contact_email = None
        self.creation_date = None
        self.derived_view = None
        self.domain = None
        self.downloads = None
        self.gisdownload_placekeywords = None
        self.jurisdiction_jurisdiction = None
        self.last_data_updated_date = None
        self.last_metadata_updated_date = None
        self.license = None
        self.owner = None
        self.owner_uid = None
        self.provenance = None
        self.publication_stage = None
        self.timeperiod_timeperiodofcontent = None
        self.timeperiod_updatefrequency = None
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
        self.date_of_most_recent_view_change_data_or_metadata = None
        self.days_since_most_recent_data_change = None
        self.days_since_last_view_change = None
        self.keyword_tags_string = None
        self.missing_metadata_fields = None
        self.number_of_rows_in_dataset = None
        self.updated_recently_enough = None

    def assemble_category_output_string(self):
        """
        Make a comma separated string of category values if is not None, otherwise value is 'NULL'
        :return:
        """
        try:
            self.category_string = ", ".join(self.theme_list) if self.theme_list is not None else var.null_string
        except TypeError as te:
            print(f"TypeError in assemble_category_output_string(): {type(self.theme_list)}, {self.theme_list}, {te}")
            self.category_string = self.theme_list

    def assemble_column_names_output_string(self):
        """
        Make a comma separated string of column names if is not None, otherwise value is 'NULL'
        :return:
        """
        self.column_names_string = ", ".join([column_dict.get("name") for column_dict in self.columns]) if self.columns is not None and 0 < len(self.columns) else var.null_string

    def assemble_keywords_output_string(self):
        """
        Make a comma separated string of keywords/tags if is not None, otherwise value is 'NULL'
        :return:
        """
        self.keyword_tags_string = ", ".join(list(self.keyword_list)) if self.keyword_list is not None and 0 < len(self.keyword_list)else var.null_string

    def assign_asset_inventory_json_to_class_values(self, asset_json):
        """
        Assign all json values to instance attributes.
        :param asset_json: asset inventory json response
        :return:
        """
        self.contact_email = asset_json.get("contact_email", None)
        self.creation_date = asset_json.get("creation_date", None)
        self.attribution = asset_json.get("attribution", None)
        self.last_metadata_updated_date = asset_json.get("last_metadata_updated_date", None)
        self.derived_view = asset_json.get("derived_view", None)
        self.domain = asset_json.get("domain", None)
        self.downloads = asset_json.get("downloads", -9999)
        self.jurisdiction_jurisdiction = asset_json.get("jurisdiction_jurisdiction", None)
        self.last_data_updated_date = asset_json.get("last_data_updated_date", None)
        self.license = asset_json.get("license", None)
        self.owner = asset_json.get("owner", None)
        self.owner_uid = asset_json.get("owner_uid", None)
        self.gisdownload_placekeywords = asset_json.get("gisdownload_placekeywords", None)
        self.provenance = asset_json.get("provenance", None)
        self.audience = asset_json.get("audience", None)
        self.publication_stage = asset_json.get("publication_stage", None)
        self.attribution_link = asset_json.get("attribution_link", None)
        self.agency_stateagencyperformingdataupdates = asset_json.get("agency_stateagencyperformingdataupdates", None)
        self.timeperiod_timeperiodofcontent = asset_json.get("timeperiod_timeperiodofcontent", None)
        self.timeperiod_updatefrequency = asset_json.get("timeperiod_updatefrequency", None)
        self.visits = asset_json.get("visits", -9999)

    def assign_data_json_to_class_values(self, dataset_json: dict):
        """
        Assign all data.json values to instance attributes
        :param dataset_json: the data.json response for all public datasets
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
        self.type_ = dataset_json.get("@type", None)
        self.theme_list = dataset_json.get("theme", None)
        self.title = dataset_json.get("title", None)

    def assign_metadata_json_to_class_values(self, metadata_json: dict):
        """
        Assign all metadata json values to instance attributes
        :param metadata_json: the metadata json response
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
        self.other_update_frequency = metadata_json.get("metadata", {}).get("custom_fields", {}).get("Time Period", {}).get(var.other_update_frequency, None)
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
        Build the metadata url string
        :return:
        """
        self.metadata_url = f"{var.md_open_data_url}/api/views/{self.four_by_four}.json"

    def build_resource_url(self):
        """
        Build the resource url string
        :return:
        """
        self.resource_url = f"{var.md_open_data_url}/resource/{self.four_by_four}.json"

    def calculate_days_since_last_data_update(self):
        """
        Subtract rows updated date in seconds from process initiation time in seconds and convert to whole days.
        :return:
        """
        self.days_since_most_recent_data_change = int(round(
                (var.process_initiation_datetime_in_seconds - self.rows_updated_at) / var.number_of_seconds_in_a_day))

    def calculate_days_since_last_view_change(self):
        """
        Subtract view updated date in seconds from process initiation time in seconds and convert to whole days.
        :return:
        """
        self.days_since_last_view_change = int(round(
            (var.process_initiation_datetime_in_seconds - self.view_last_modified) / var.number_of_seconds_in_a_day))

    def calculate_number_of_rows_in_dataset(self):
        """
        Extract null/non-null values from metadata json and sum to determine number of rows in dataset

        NOTE: The 'non_null' key is missing from some datasets. It may be necessary to add more functionality that
            makes requests when the json values are not available in order to determine the number of rows. A database
            flag value of -9999 is currently used to indicate non-existent row counts.
        :return:
        """
        first_column_dict = self.columns[0] if 0 < len(self.columns) else {}
        cached_contents_dict = first_column_dict.get("cachedContents", {})
        try:
            self.number_of_rows_in_dataset = sum([int(cached_contents_dict.get("non_null")),
                                              int(cached_contents_dict.get("null"))]) if 0 < len(cached_contents_dict) else -9999
        except TypeError as te:
            self.number_of_rows_in_dataset = -9999
        # TODO: For datasets that don't supply a non_null count then may need to make requests to actual dataset and count
        #   the number of records. This is more efficient than doing so for every single dataet. Only make costly web requests for subset of all datasets.

    def check_for_null_source_url_and_replace(self):
        """
        Replace null source link values with a value Socrata recognizes as a valid url
        :return:
        """
        self.attribution_link = "https://N.U.LL" if self.attribution_link is None else self.attribution_link

    def determine_date_of_most_recent_data_change(self):
        """
        Determine the date of the most recent data change, based off of the rows_update_at value.
        When a rows_updated_at value is not present the publication_date was chosen as a substitute. The choices
        made in this function originated in the original Date Freshness process.
        :return:
        """
        if self.rows_updated_at is None:
            self.rows_updated_at = self.publication_date
        self.date_of_most_recent_data_change = datetime.datetime.fromtimestamp(self.rows_updated_at)

    def determine_date_of_most_recent_view_change(self):
        """
        Determine the date of the most recent view change, based off of the views_last_modified.
        When a views_last_modified value is not present the publication_date was chosen as a substitute. The choices
        made in this function originated in the original Date Freshness process.
        :return:
        """
        if self.view_last_modified is None:
            self.view_last_modified = self.publication_date
        self.date_of_most_recent_view_change_data_or_metadata = datetime.datetime.fromtimestamp(self.rows_updated_at)

    def determine_missing_metadata_fields(self, asset_json):
        """
        Compare values in asset inventory json to a default set of possible metadata values to determine missing values.
        :param asset_json: asset inventory json on socrata dataset
        :return:
        """
        if len(asset_json) == 0:
            self.missing_metadata_fields = "Error generating missing metadata list"
        else:
            full_exmaple_set_of_keys = set(var.expected_socrata_asset_inventory_json_keys_dict.keys())
            included_asset_json_keys_set = set(asset_json.keys())
            difference = full_exmaple_set_of_keys.difference(included_asset_json_keys_set)
            self.missing_metadata_fields = ", ".join(
                [var.expected_socrata_asset_inventory_json_keys_dict.get(value) for value in difference]) if 0 < len(
                difference) else "All Fields Present"
        return

    def extract_four_by_four(self):
        """
        Extract the four-by-four dataset id value from the landing page url
        :return:
        """
        self.four_by_four = os.path.basename(self.landing_page)
        return None

    def is_up_to_date(self):
        """
        Determine if a dataset is up to date according to its update frequency.
        Created two dictionaries, instead of one, to hold integer comparison value snd string values. The integer
        values are checked against the number of days since the data has been updated. If the update frequency value
        is a string then retrieve the string value from the string dict. If no value is found in the dicts then the
        metadata is deemed as missing.
        :return:
        """

        updated_enough_ints = {"Monthly": 31, "Every 10 Years": 3650,
                               "Annually": 365, "Quarterly": 91, "Continually": 31, "Weekly": 7, "Daily": 1,
                               "Triennially (Every Three Years)": 1095, "Biannually": 730,
                               "Semiannually (Twice per Year)": 183}
        updated_enough_strings = {"Static Data": var.updated_enough_yes,
                                  "Static Cut": var.evaluation_difficult,
                                  "As Needed": var.evaluation_difficult,
                                  var.all_map_layers: f"{var.better_metadata_needed} {var.whether_dataset}",
                                  "": f"{var.better_metadata_needed} {var.update_frequency_missing}"}
        answer = None

        int_check = updated_enough_ints.get(self.timeperiod_updatefrequency, None)
        string_check = updated_enough_strings.get(self.timeperiod_updatefrequency, None)

        if int_check is not None:
            answer = var.updated_enough_yes if self.days_since_most_recent_data_change <= int_check else var.updated_enough_no
        elif string_check is not None:
            answer = string_check
        else:
            answer = var.metadata_missing

        self.updated_recently_enough = answer
        return

    def passes_filter_data_json(self, gis_counter: itertools.count, dataset_freshness_counter: itertools.count):
        """
        Determine if a dataset is to be processed or passed on based on the title.
        All GIS datasets are not processed in the Socrata portion of the process because these "datasets" are just
        a reference to ArcGIS Online. The dataset freshness report is skipped so as to not self inspect. The
        'Homepage Categories' value was in the original design. On testing, none of these were encountered but to be
        safe the check was included in this redesign.
        :param gis_counter: itertools counter for gis dataset tracking
        :param dataset_freshness_counter: itertools counter for any data freshenss dataset tracking
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

    def process_update_frequency(self):
        """
        Process the update frequency value if is equal to test condition and substitute appropriate value.
        :return:
        """
        if self.timeperiod_updatefrequency is None:
            self.timeperiod_updatefrequency = var.null_string
        elif self.timeperiod_updatefrequency == var.please_describe_below and self.other_update_frequency is not None:
            self.timeperiod_updatefrequency = self.other_update_frequency
        else:
            pass
        return

    @staticmethod
    def create_socrata_client(domain: str, app_token: str, username:str, password: str) -> Socrata:
        """
        Create and return a Socrata client for use.

        NOTE_1: It is absolutely essential the the domain be a domain and not a url; 'https://opendata.maryland.gov'
            will not substitute for 'opendata.maryland.gov'. Loose use of the term domain will get cause you grief.

        :param domain: domain for maryland open data portal.
        :param app_token: application token for throttling limitations
        :param username: account username
        :param password: password for account
        :return: Socrata connection client
        """

        return Socrata(domain=domain, app_token=app_token, username=username, password=password)

    @staticmethod
    def upsert_to_socrata(client: Socrata, dataset_identifier: str, zipper: dict) -> None:
        """
        Upsert data to Socrata dataset.

        :param client: Socrata connection client
        :param dataset_identifier: Unique Socrata dataset identifier. Not the data page identifier but primary page id.
        :param zipper: dictionary of zipped results (headers and data values)
        :return: None
        """
        try:
            client.upsert(dataset_identifier=dataset_identifier, payload=zipper, content_type='json')
        except Exception as e:
            print("Error upserting to Socrata: {}. {}".format(dataset_identifier, e))
        return

    @staticmethod
    def request_and_aggregate_all_socrata_records(client: Socrata, fourbyfour: str,
                                                  limit_max_and_offset: int = LIMIT_MAX_AND_OFFSET) -> list:
        """
        Make web requests and accumulate records until no more are returned for the dataset of interest
        For datasets with a lot of records it is necessary to make multiple requests to Socrata.
        :param client: Socrata Client
        :param fourbyfour: the XXXX-XXXX id that socrata uses to identify an asset
        :param limit_max_and_offset: number or records to return and the value used to offset requests to get
            next batch of records in a large dataset.
        :return: returns a list of json/dicts for datasets
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

    @staticmethod
    def process_audience_to_bool(audience_str: str) -> bool:
        """
        Process string values to boolean based on exchange dict

        After Socrata changed the Asset Inventory 'public' became 'audience' and values changed from bool to string.
        This function exchanges the string for a bool
        :param audience_str: audience value extracted from asset inventory response
        :return: bool
        """
        audience_options_dict = {"public": True, "private": False, "internal": False}
        return audience_options_dict.get(audience_str, None)
