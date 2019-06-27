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

        self.admin = None
        self.member = None
        self.other = None

        # From within 'other'
        self.group_id = None
        self.group_title = None
        self.is_invitation_only = None
        self.group_owner = None
        self.group_description = None
        self.group_snippet = None
        self.group_tags = None
        self.is_view_only = None
        self.is_open_data = None
        self.group_created = None
        self.group_modified = None
        self.group_access = None
        self.group_capabilities = None

    def assign_group_json_to_class_values(self, group_json: dict):
        self.admin = group_json.get("admin", None)
        self.member = group_json.get("member", None)
        self.other = group_json.get("other", None)
        if self.other is not None:
            self.group_id = group_json.get("id", None)
            self.group_title = group_json.get("title", None)
            self.is_invitation_only = group_json.get("isInvitationOnly", None)
            self.group_owner = group_json.get("owner", None)
            self.group_description = group_json.get("description", None)
            self.group_snippet = group_json.get("snippet", None)
            self.group_tags = group_json.get("tags", None)
            self.is_view_only = group_json.get("isViewOnly", None)
            self.is_open_data = group_json.get("isOpenData", None)
            self.group_created = group_json.get("created", None)
            self.group_modified = group_json.get("modified", None)
            self.group_access = group_json.get("access", None)
            self.group_capabilities = group_json.get("capabilities", None)

