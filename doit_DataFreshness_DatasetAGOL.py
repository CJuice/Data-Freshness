"""

"""
import DataFreshness.doit_DataFreshness_Variables_Socrata as var
from DataFreshness.doit_DataFreshness_Utility import Utility
import json


class DatasetAGOL:
    """

    """
    OWNER = 'owner:mdimapdatacatalog'
    RECORD_LIMIT = '100' # When changed from 100 the return quanity doesn't actually change. Unsure why ineffective.
    SORT_FIELD = 'title'

    def __init__(self):

        # NON-DERIVED
        self.access = None
        self.access_information = None
        self.app_categories = None
        self.average_rating = None
        self.banner = None
        self.categories = None
        self.content_status = None
        self.created = None
        self.culture = None
        self.description = None
        self.documentation = None
        self.extent = None
        self.group_designations = None
        self.guid = None
        self.id = None
        self.industries = None
        self.languages = None
        self.large_thumbnail = None
        self.license_info = None
        self.listed = None
        self.modified = None
        self.name = None
        self.number_of_comments = None
        self.number_of_ratings = None
        self.number_of_views = None
        self.org_id = None
        self.owner = None
        self.properties = None
        self.proxy_filter = None
        self.score_completeness = None
        self.screenshots = None
        self.size = None
        self.snippet = None
        self.spatial_reference = None
        self.tags = None
        self.thumbnail = None
        self.title = None
        self.type_ = None
        self.type_keywords = None
        self.url = None

        # DERIVED
        self.standardized_url = None
        self.standardized_url_2 = None

    def assign_data_catalog_json_to_class_values(self, data_json: dict):
        self.access = data_json.get("access", None)
        self.access_information = data_json.get("accessInformation", None)
        self.app_categories = data_json.get("appCategories", None)
        self.average_rating = data_json.get("avgRating", None)
        self.banner = data_json.get("banner", None)
        self.categories = data_json.get("categories", None)
        self.content_status = data_json.get("contentStatus", None)
        self.created = data_json.get("created", None)
        self.culture = data_json.get("culture", None)
        self.description = data_json.get("description", None)
        self.documentation = data_json.get("documentation", None)
        self.extent = data_json.get("extent", None)
        self.group_designations = data_json.get("groupDesignations", None)
        self.guid = data_json.get("guid", None)
        self.id = data_json.get("id", None)
        self.industries = data_json.get("industries", None)
        self.languages = data_json.get("languages", None)
        self.large_thumbnail = data_json.get("largeThumbnail", None)
        self.license_info = data_json.get("licenseInfo", None)
        self.listed = data_json.get("listed", None)
        self.modified = data_json.get("modified", None)
        self.name = data_json.get("name", None)
        self.number_of_comments = data_json.get("numComments", None)
        self.number_of_ratings = data_json.get("numRatings", None)
        self.number_of_views = data_json.get("numViews", None)
        self.org_id = data_json.get("orgId", None)
        self.owner = data_json.get("owner", None)
        self.properties = data_json.get("properties", None)
        self.proxy_filter = data_json.get("proxyFilter", None)
        self.score_completeness = data_json.get("scoreCompleteness", None)
        self.screenshots = data_json.get("screenshots", None)
        self.size = data_json.get("size", None)
        self.snippet = data_json.get("snippit", None)
        self.spatial_reference = data_json.get("spatialReference", None)
        self.tags = data_json.get("tags", None)
        self.thumbnail = data_json.get("thumbnail", None)
        self.title = data_json.get("title", None)
        self.type_ = data_json.get("type", None)
        self.type_keywords = data_json.get("typeKeywords", None)
        self.url = data_json.get("url", None)

    def build_standardized_url(self):
        # FIXME: This functionality was taken from the old process but it only works for really basic titles. Special
        #   characters cause bad url's. Need to resolve what the purpose of this url should be: send user to data.imap.maryland.gov
        #   or send to the application wherever it is hosted. We have the url provided in response to agol but these are
        #   not standardized. If we want a clean look we should maybe use the id to build the agol url for it.

        # EXISTING METHOD
        # groomed_title = self.title.replace("- ", "").replace(" ", "-").lower()
        # self.standardized_url = f"https://data.imap.maryland.gov/datasets/{groomed_title}"

        # NEW OPTIONS TO EXPLORE
        self.standardized_url = f"https://maryland.maps.arcgis.com/home/item.html?id={self.id}"

    # def build_arcgis_request_data_dict(self, start_num: int = None) -> dict:
    #     return {
    #         'q': DatasetAGOL.OWNER,
    #         'num': DatasetAGOL.RECORD_LIMIT,
    #         'start': start_num,
    #         'sortField': DatasetAGOL.SORT_FIELD,
    #         'f': 'json'
    #     }

    @staticmethod
    def request_all_data_catalog_results(url: str) -> list:
        """
        Make web requests and accumulate records until no more are returned for the dataset of interest

        :return: returns a list of json/dicts for datasets
        """
        more_records_exist = True
        # total_record_count = 0
        start_number = 0
        master_list_of_dicts = []

        while more_records_exist:
            # request_cycle_record_count = 0
            data = {'q': DatasetAGOL.OWNER,
                    'num': DatasetAGOL.RECORD_LIMIT,
                    'start': start_number,
                    'sortField': DatasetAGOL.SORT_FIELD,
                    'f': 'json'
                    }
            response = Utility.request_POST(url=url, data=data)
            try:
                resp_json = response.json()
            except json.JSONDecodeError as jse:
                print(f"JSONDecodeError after making post request to arcgis online. url={url}, data={data} {jse}")
                exit()
            else:
                results = resp_json.get("results", {})
                start_number = resp_json.get("nextStart", None)
                master_list_of_dicts.extend(results)
                # print(f"Start Number: {start_number}")

                # number_of_records_returned = len(results)
                # request_cycle_record_count += number_of_records_returned
                # total_record_count += number_of_records_returned

            # AGOL nextStart equal to -1 must indicate end of records reached
            if start_number == -1:
                more_records_exist = False

        return master_list_of_dicts