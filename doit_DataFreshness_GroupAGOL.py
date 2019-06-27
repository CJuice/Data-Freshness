"""

"""


class GroupAGOL:

    def __init__(self):
        """
        phone - not stored
        sortField - not stored
        sortOrder - not stored
        thumbnail - not stored
        isFav - not stored
        isReadOnly - not stored
        protected - not stored
        autoJoin - not stored
        notificationsEnabled - not stored
        provider - not stored
        providerGroupName - not stored
        leavingDisallowed - not stored
        hiddenMembers - not stored
        displaySettings - not stored
            itemTypes - not stored
        """

        # NOT DERIVED
        self.admin = None
        self.member = None
        self.other_list = None
        # An assumption has been made that the list is only len = 1.
        #   From existing data freshness only ever saw one group.
        #   From inspection of 900+ datasets was only ever len = 1. 20190626 CJuice
        self.other_dict = None

        #   From within 'other'
        self.group_id = None
        self.group_title = None
        self.is_invitation_only = None
        self.group_owner = None
        self.group_description_raw = None
        self.group_snippet_raw = None
        self.group_tags = None
        self.is_view_only = None
        self.is_open_data = None
        self.group_created = None
        self.group_modified = None
        self.group_access = None
        self.group_capabilities = None

        # DERIVED
        self.group_description_text = None
        self.group_created_dt = None
        self.group_modified_dt = None

    def assign_group_json_to_class_values(self, group_json: dict):
        self.admin = group_json.get("admin", None)
        self.member = group_json.get("member", None)
        self.other_list = group_json.get("other", None)

        if self.other_list is not None and 0 < len(self.other_list):
            self.other_dict = self.other_list[0]
            self.group_id = self.other_dict.get("id", None)
            self.group_title = self.other_dict.get("title", None)
            self.is_invitation_only = self.other_dict.get("isInvitationOnly", None)
            self.group_owner = self.other_dict.get("owner", None)
            self.group_description_raw = self.other_dict.get("description", None)
            self.group_snippet_raw = self.other_dict.get("snippet", None)
            self.group_tags = self.other_dict.get("tags", None)
            self.is_view_only = self.other_dict.get("isViewOnly", None)
            self.is_open_data = self.other_dict.get("isOpenData", None)
            self.group_created = self.other_dict.get("created", None)
            self.group_modified = self.other_dict.get("modified", None)
            self.group_access = self.other_dict.get("access", None)
            self.group_capabilities = self.other_dict.get("capabilities", None)

