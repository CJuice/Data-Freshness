"""

"""
import DataFreshness.doit_DataFreshness_Variables_AGOL as var
from DataFreshness.doit_DataFreshness_Utility import Utility
import json
import datetime
from dateutil import parser as date_parser
from bs4 import BeautifulSoup


class DatasetAGOL:
    """

    """

    OWNER = 'owner:mdimapdatacatalog'
    RECORD_LIMIT = '100' # When changed from 100 the return quanity doesn't actually change. Unsure why ineffective.
    SORT_FIELD = 'title'

    def __init__(self):
        """
                Instantiate the default dataset object in preparation for value assignments.

                Values in metadata xml (for example) that were previously sourced from data catalog have been ignored.
                 The following listing captures the decisions made while building this process.

                d = data catalog, m = metadata

                DATA CATALOG NOTES:
                appCategories - dropped after inspection. All values Null
                banner - dropped after inspection. All values Null
                categories - dropped after inspection. All values Null
                culture - dropped after inspection. All values en-us
                documentation - dropped after inspection. All values Null
                groupDesignations - dropped after inspection. All values Null
                guid - dropped after inspection. All values Null
                industries - dropped after inspection. All values Null
                languages - dropped after inspection. All values Null
                largeThumbnail - dropped storage because doesn't appear to have much value at this time
                listed - dropped after inspection. All values Null
                proxyFilter - dropped after inspection. All values Null
                screenshots - dropped after inspection. All values Null
                size - dropped after inspection. All values Null
                snippet - dropped after inspection. All values Null
                thumbnail - dropped storage because doesn't appear to have much value at this time

                METADATA NOTES:
                dataIdInfo
                    resTitle - title (d)
                    idAbs - description (d)
                    idPurp - title (d) or snippit (d) (appears to just be the title value)
                    idCredit - accessInformation (d)
                    dataChar>CharSetDc - Not stored (unknown meaning or value)
                    searchKeys>keyword - tags (d)
                    resConst>Consts>useLimit - licenseInfo (d)
                dataExt>geoEle>GeoBndBox> - Not stored (Appears to be bounding box coordinates). Not present in all.
                distInfo
                    distTranOps>onLineSrc>linkage - url (d)
                Esri
                    ArcGISStyle - Not Stored (metadata style indicator)
                    ArcGISFormat - Not Stored (unknown meaning)
                    ArcGISProfile - Not Stored (unknown value)
                    PublishStatus - Not Stored (unknown meaning or value)
                mdDateSt - Not Stored (unknonw meaning or value)
                mdFileID - id (d)
                mdChar>CharSetCd - Not Stored (uknown meaning or value)
                mdContact>role<RoleCd - Not Stored (uknown meaning or value)
                Binary - Not Stored (appears to be for images like thumbnails). Not present in all.

                XML NOTES:
                    From examining every asset as of 20190621 CJuice. I did not explore every nook and cranny. There
                    could be remaining values present to exploit but deep comparison of the xml for every asset
                    would need to occur.
                    set of root xml elements -
                        {'mdChar', 'Esri', 'mdContact', 'mdDateSt', 'dataIdInfo', 'distInfo', 'Binary', 'mdFileID'}
                    set of children xml elements under dataIdInfo -
                        {'resConst', 'dataExt', 'searchKeys', 'dataChar', 'idAbs', 'idPurp', 'idCitation', 'idCredit'}
                    set of children xml elements under idCitation -
                        {'date', 'citRespParty', 'resTitle', 'collTitle'}
                    set of children xml elements under date -
                        {'reviseDate', 'pubDate'}

        """

        # NON-DERIVED
        # Data Catalog sourced attributes
        self.access = None
        self.access_information = None
        # self.app_categories = None
        self.average_rating = None
        # self.banner = None
        # self.categories = None
        self.content_status = None
        self.created = None
        # self.culture = None
        self.description = None
        # self.documentation = None
        self.extent = None
        # self.group_designations = None
        # self.guid = None
        self.id = None
        # self.industries = None
        # self.languages = None
        # self.large_thumbnail = None
        self.license_info = None
        # self.listed = None
        self.modified = None
        self.name = None
        self.number_of_comments = None
        self.number_of_ratings = None
        self.number_of_views = None
        self.org_id = None
        self.owner = None
        self.properties = None
        # self.proxy_filter = None
        self.score_completeness = None
        # self.screenshots = None
        # self.size = None
        # self.snippet = None
        self.spatial_reference = None
        self.tags = None
        # self.thumbnail = None
        self.title = None
        self.type_ = None
        self.type_keywords = None
        self.url = None

        # Metadata XML sourced attributes
        # self.esri_metadata_xml_element = None
        self.maintenance_frequency_code = None
        self.meta_creation_date = None
        self.meta_creation_time = None
        self.meta_modification_date = None
        self.meta_modification_time = None
        self.organization_name = None
        self.publication_date = None

        # DERIVED
        self.created_dt = None
        self.description_text = None
        self.license_info_text = None
        self.meta_creation_date_dt = None
        self.meta_creation_time_dt = None
        self.meta_modification_date_dt = None
        self.meta_modification_time_dt = None
        self.metadata_url = None
        self.modified_dt = None
        self.publication_date_dt = None
        self.standardized_url = None

    def assign_data_catalog_json_to_class_values(self, data_json: dict):
        self.access = data_json.get("access", None)
        self.access_information = data_json.get("accessInformation", None)
        # self.app_categories = data_json.get("appCategories", None)
        self.average_rating = data_json.get("avgRating", None)
        # self.banner = data_json.get("banner", None)
        # self.categories = data_json.get("categories", None)
        self.content_status = data_json.get("contentStatus", None)
        self.created = data_json.get("created", None)
        # self.culture = data_json.get("culture", None)
        self.description = data_json.get("description", None)
        # self.documentation = data_json.get("documentation", None)
        self.extent = data_json.get("extent", None)
        # self.group_designations = data_json.get("groupDesignations", None)
        # self.guid = data_json.get("guid", None)
        self.id = data_json.get("id", None)
        # self.industries = data_json.get("industries", None)
        # self.languages = data_json.get("languages", None)
        # self.large_thumbnail = data_json.get("largeThumbnail", None)
        self.license_info = data_json.get("licenseInfo", None)
        # self.listed = data_json.get("listed", None)
        self.modified = data_json.get("modified", None)
        self.name = data_json.get("name", None)
        self.number_of_comments = data_json.get("numComments", None)
        self.number_of_ratings = data_json.get("numRatings", None)
        self.number_of_views = data_json.get("numViews", None)
        self.org_id = data_json.get("orgId", None)
        self.owner = data_json.get("owner", None)
        self.properties = data_json.get("properties", None)
        # self.proxy_filter = data_json.get("proxyFilter", None)
        self.score_completeness = data_json.get("scoreCompleteness", None)
        # self.screenshots = data_json.get("screenshots", None)
        # self.size = data_json.get("size", None)
        # self.snippet = data_json.get("snippit", None)
        self.spatial_reference = data_json.get("spatialReference", None)
        self.tags = data_json.get("tags", None)
        # self.thumbnail = data_json.get("thumbnail", None)
        self.title = data_json.get("title", None)
        self.type_ = data_json.get("type", None)
        self.type_keywords = data_json.get("typeKeywords", None)
        self.url = data_json.get("url", None)

    def build_standardized_item_url(self):
        # Spoke with Matt, this is what we are going with.
        self.standardized_url = var.arcgis_item_url.format(item_id=self.id)

    def build_metadata_xml_url(self):
        self.metadata_url = var.arcgis_metadata_url.format(arcgis_sharing_rest_url=var.arcgis_sharing_rest_url,
                                                           item_id=self.id)

    def convert_milliseconds_attributes_to_datetime(self):

        try:
            self.created_dt = datetime.datetime.fromtimestamp(self.created/1000)
        except TypeError as te:
            print(f"TypeError during convert_milliseconds_to_datetime(). millis value:{self.created}, {te}")
        try:
            self.modified_dt = datetime.datetime.fromtimestamp(self.modified/1000)
        except TypeError as te:
            print(f"TypeError during convert_milliseconds_to_datetime(). millis value:{self.modified}, {te}")

    def extract_and_assign_esri_date_time_values(self, element):
        """

        :param element:
        :return:
        """
        esri_metadata_xml_element = Utility.extract_first_immediate_child_feature_from_element(
            element=element,
            tag_name="Esri")

        if esri_metadata_xml_element is None:
            print(f"ESRI XML Tag is None. Asset: {self.standardized_url}")
            return

        esri_xml_tags_and_values = {"CreaDate": None,
                                    "CreaTime": None,
                                    "ModDate": None,
                                    "ModTime": None}

        for tag_name, value in esri_xml_tags_and_values.items():
            try:
                esri_xml_tags_and_values[tag_name] = Utility.extract_first_immediate_child_feature_from_element(
                    element=esri_metadata_xml_element,
                    tag_name=tag_name).text
            except AttributeError as ae:
                print(f"ESRI XML Tag '{tag_name}' NOT FOUND. Call to .text raised Attribute Error: {ae}. Asset: {self.standardized_url}")

        self.meta_creation_date = esri_xml_tags_and_values.get("CreaDate")
        self.meta_creation_time = esri_xml_tags_and_values.get("CreaTime")
        self.meta_modification_date = esri_xml_tags_and_values.get("ModDate")
        self.meta_modification_time = esri_xml_tags_and_values.get("ModTime")

        return

    def extract_and_assign_maintenance_frequency(self, element):
        """

        After the following dataIdInfo/resMaint/maintFreq/MaintFreqCd
        :param element:
        :return:
        """
        data_id_info_element = Utility.extract_first_immediate_child_feature_from_element(element=element, tag_name="dataIdInfo") if element is not None else None
        res_maintenance_element = Utility.extract_first_immediate_child_feature_from_element(element=data_id_info_element, tag_name="resMaint") if data_id_info_element is not None else None
        maint_freq_element = Utility.extract_first_immediate_child_feature_from_element(element=res_maintenance_element, tag_name="maintFreq") if res_maintenance_element is not None else None
        maint_freq_code_element = Utility.extract_first_immediate_child_feature_from_element(element=maint_freq_element, tag_name="maintFreqCd") if maint_freq_element is not None else None
        self.maintenance_frequency_code = maint_freq_code_element.text if maint_freq_code_element is not None else None

    def extract_and_assign_organization_name(self, element):
        """

        After the following dataIdInfo/idCitation/citRespParty/rpOrgName
        :param element:
        :return:
        """
        data_id_info_element = Utility.extract_first_immediate_child_feature_from_element(element=element,
                                                                                          tag_name="dataIdInfo") if element is not None else None
        id_citation_element = Utility.extract_first_immediate_child_feature_from_element(element=data_id_info_element,
                                                                                         tag_name="idCitation") if data_id_info_element is not None else None
        cit_resp_party_element = Utility.extract_first_immediate_child_feature_from_element(element=id_citation_element,
                                                                                  tag_name="citRespParty") if id_citation_element is not None else None
        rp_org_name_element = Utility.extract_first_immediate_child_feature_from_element(element=cit_resp_party_element,
                                                                                      tag_name="rpOrgName") if cit_resp_party_element is not None else None
        self.organization_name = rp_org_name_element.text if rp_org_name_element is not None else None

    def extract_and_assign_publication_date(self, element):
        """

        After the following dataIdInfo/idCitation/date/pubDate
        :param element:
        :return:
        """
        data_id_info_element = Utility.extract_first_immediate_child_feature_from_element(element=element, tag_name="dataIdInfo") if element is not None else None
        id_citation_element = Utility.extract_first_immediate_child_feature_from_element(element=data_id_info_element, tag_name="idCitation") if data_id_info_element is not None else None
        date_element = Utility.extract_first_immediate_child_feature_from_element(element=id_citation_element, tag_name="date") if id_citation_element is not None else None
        pub_date_element = Utility.extract_first_immediate_child_feature_from_element(element=date_element, tag_name="pubDate") if date_element is not None else None
        self.publication_date = pub_date_element.text if pub_date_element is not None else None

    def parse_date_like_string_attributes(self):
        """

        NOTE: The attributes of interest at the time of design were esri's creation date and time, esri's modification
        date and time, and the publication date that is auto-populated unless we manually enter a value
        :return:
        """
        def local_inner_function(attribute_name, value):
            """

            :param attribute_name:
            :param value:
            :return:
            """
            try:
                return date_parser.parse(value)
            except (ValueError, TypeError) as err:
                print(f"Unable to parse {attribute_name} value: {value}, {err}")
                return None

        self.meta_creation_date_dt = local_inner_function(attribute_name="meta_creation_date",
                                                          value=self.meta_creation_date)
        self.meta_creation_time_dt = local_inner_function(attribute_name="meta_creation_time",
                                                          value=self.meta_creation_time)
        self.meta_modification_date_dt = local_inner_function(attribute_name="meta_modification_date",
                                                              value=self.meta_modification_date)
        self.meta_modification_time_dt = local_inner_function(attribute_name="meta_modification_time",
                                                              value=self.meta_modification_time)
        self.publication_date_dt = local_inner_function(attribute_name="publication_date", value=self.publication_date)

    def parse_html_attribute_values_to_soup_get_text(self):
        """

        :return:
        """
        def local_inner_function(attribute_name, value):
            """

            :param attribute_name:
            :param value:
            :return:
            """
            try:
                soup = BeautifulSoup(self.license_info, "html.parser")
                return soup.get_text()
            except Exception as e:
                print(f"Unanticipated Exception raised in parsing license_info using BeautifulSoup {e}")
                return None
        self.license_info_text = local_inner_function(attribute_name="license_info", value=self.license_info)
        self.description_text = local_inner_function(attribute_name="description", value=self.description)


    @staticmethod
    def request_all_data_catalog_results() -> list:
        """
        Make web requests and accumulate records until no more are returned for the dataset of interest

        :return: returns a list of json/dicts for datasets
        """
        more_records_exist = True
        start_number = 0
        master_list_of_dicts = []

        while more_records_exist:
            data = {'q': DatasetAGOL.OWNER,
                    'num': DatasetAGOL.RECORD_LIMIT,
                    'start': start_number,
                    'sortField': DatasetAGOL.SORT_FIELD,
                    'f': 'json'
                    }
            response = Utility.request_POST(url=var.arcgis_data_catalog_url, data=data)
            try:
                resp_json = response.json()
            except json.JSONDecodeError as jse:
                print(f"JSONDecodeError after making post request to arcgis online. url={var.arcgis_data_catalog_url}, data={data} {jse}")
                exit()
            else:
                results = resp_json.get("results", {})
                start_number = resp_json.get("nextStart", None)
                master_list_of_dicts.extend(results)
                # print(f"Start Number: {start_number}")

            # AGOL nextStart equal to -1 must indicate end of records reached
            if start_number == -1:
                more_records_exist = False

        return master_list_of_dicts

