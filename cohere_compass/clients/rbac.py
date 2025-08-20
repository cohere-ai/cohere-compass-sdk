import json
from typing import TypeVar

import requests
from pydantic import BaseModel
from requests import HTTPError

from cohere_compass.models import (
    GroupCreateRequest,
    GroupCreateResponse,
    GroupDeleteResponse,
    GroupFetchResponse,
    GroupUserDeleteResponse,
    PolicyRequest,
    RoleCreateRequest,
    RoleCreateResponse,
    RoleDeleteResponse,
    RoleFetchResponse,
    RoleMappingDeleteResponse,
    RoleMappingRequest,
    RoleMappingResponse,
    UserCreateRequest,
    UserCreateResponse,
    UserDeleteResponse,
    UserFetchResponse,
)


class CompassRootClient:
    """
    TO BE DEPRECATED.

    Client for interacting with Compass RBAC API V1 (now Legacy) as a root user.

    Prefer the CompassRootClient in access_control.py for new development.
    """

    def __init__(
        self, compass_url: str, root_user_token: str, include_api_in_url: bool = True
    ):
        """
        Initialize a new CompassRootClient.

        :param compass_url: URL of the Compass instance.
        :param root_user_token: Root user token for Compass instance.
        :param include_api_in_url: Whether to include '/api' in the base URL.
               Defaults to True.
        """
        self.base_url = (
            compass_url
            + ("/api" if include_api_in_url else "")
            + "/security/admin/rbac"
        )
        self.headers = {
            "Authorization": f"Bearer {root_user_token}",
            "Content-Type": "application/json",
        }

    T = TypeVar("T", bound=BaseModel)
    U = TypeVar("U", bound=BaseModel)
    Headers = dict[str, str]

    @staticmethod
    def _fetch_entities(url: str, headers: Headers, entity_type: type[T]) -> list[T]:
        response = requests.get(url, headers=headers)
        CompassRootClient.raise_for_status(response)
        return [entity_type.model_validate(entity) for entity in response.json()]

    @staticmethod
    def _create_entities(
        url: str, headers: Headers, entity_request: list[T], entity_response: type[U]
    ) -> list[U]:
        response = requests.post(
            url,
            json=[json.loads(entity.model_dump_json()) for entity in entity_request],
            headers=headers,
        )
        CompassRootClient.raise_for_status(response)
        return [
            entity_response.model_validate(response) for response in response.json()
        ]

    @staticmethod
    def _delete_entities(
        url: str, headers: Headers, names: list[str], entity_response: type[U]
    ) -> list[U]:
        entities = ",".join(names)
        response = requests.delete(f"{url}/{entities}", headers=headers)
        CompassRootClient.raise_for_status(response)
        return [entity_response.model_validate(entity) for entity in response.json()]

    def fetch_users(self) -> list[UserFetchResponse]:
        """
        Fetch all users from Compass.

        :returns: A list containing the users.
        """
        return self._fetch_entities(
            f"{self.base_url}/v1/users", self.headers, UserFetchResponse
        )

    def fetch_groups(self) -> list[GroupFetchResponse]:
        """
        Fetch all groups from Compass.

        :returns: A list containing the groups.
        """
        return self._fetch_entities(
            f"{self.base_url}/v1/groups", self.headers, GroupFetchResponse
        )

    def fetch_roles(self) -> list[RoleFetchResponse]:
        """
        Fetch all roles from Compass.

        :returns: A list containing the roles.
        """
        return self._fetch_entities(
            f"{self.base_url}/v1/roles", self.headers, RoleFetchResponse
        )

    def fetch_role_mappings(self) -> list[RoleMappingResponse]:
        """
        Fetch all role mappings from Compass.

        :returns: A list containing the role mappings.
        """
        return self._fetch_entities(
            f"{self.base_url}/v1/role-mappings", self.headers, RoleMappingResponse
        )

    def create_users(
        self, *, users: list[UserCreateRequest]
    ) -> list[UserCreateResponse]:
        """
        Create new users in Compass.

        :param users: List of users to be created.

        :returns: A list containing the created users.
        """
        return self._create_entities(
            url=f"{self.base_url}/v1/users",
            headers=self.headers,
            entity_request=users,
            entity_response=UserCreateResponse,
        )

    def create_groups(
        self, *, groups: list[GroupCreateRequest]
    ) -> list[GroupCreateResponse]:
        """
        Create new groups in Compass.

        :param groups: List of groups to be created.

        :returns: A list containing the created groups.
        """
        return self._create_entities(
            url=f"{self.base_url}/v1/groups",
            headers=self.headers,
            entity_request=groups,
            entity_response=GroupCreateResponse,
        )

    def create_roles(
        self, *, roles: list[RoleCreateRequest]
    ) -> list[RoleCreateResponse]:
        """
        Create new roles in Compass.

        :param roles: List of roles to be created.

        :returns: A list containing the created roles.
        """
        return self._create_entities(
            url=f"{self.base_url}/v1/roles",
            headers=self.headers,
            entity_request=roles,
            entity_response=RoleCreateResponse,
        )

    def create_role_mappings(
        self, *, role_mappings: list[RoleMappingRequest]
    ) -> list[RoleMappingResponse]:
        """
        Create new role mappings in Compass.

        :param role_mappings: List of role mappings to be created.

        :returns: A list containing the created role mappings.
        """
        return self._create_entities(
            url=f"{self.base_url}/v1/role-mappings",
            headers=self.headers,
            entity_request=role_mappings,
            entity_response=RoleMappingResponse,
        )

    def delete_users(self, *, user_names: list[str]) -> list[UserDeleteResponse]:
        """
        Delete users from Compass.

        :param user_names: List of user names to be deleted.

        :returns: A list containing the deleted users.
        """
        return self._delete_entities(
            f"{self.base_url}/v1/users", self.headers, user_names, UserDeleteResponse
        )

    def delete_groups(self, *, group_names: list[str]) -> list[GroupDeleteResponse]:
        """
        Delete groups from Compass.

        :param group_names: List of group names to be deleted.

        :returns: A list containing the deleted groups.
        """
        return self._delete_entities(
            f"{self.base_url}/v1/groups", self.headers, group_names, GroupDeleteResponse
        )

    def delete_roles(self, *, role_ids: list[str]) -> list[RoleDeleteResponse]:
        """
        Delete roles from Compass.

        :param role_ids: List of role IDs to be deleted.

        :returns: A list containing the deleted roles.
        """
        return self._delete_entities(
            f"{self.base_url}/v1/roles", self.headers, role_ids, RoleDeleteResponse
        )

    def delete_role_mappings(
        self, *, role_name: str, group_name: str
    ) -> list[RoleMappingDeleteResponse]:
        """
        Delete role mappings from Compass.

        :param role_name: Name of the role.
        :param group_name: Name of the group.

        :returns: A list containing the deleted role mappings.
        """
        response = requests.delete(
            f"{self.base_url}/v1/role-mappings/role/{role_name}/group/{group_name}",
            headers=self.headers,
        )
        self.raise_for_status(response)
        return [
            RoleMappingDeleteResponse.model_validate(role_mapping)
            for role_mapping in response.json()
        ]

    def delete_user_group(
        self, *, group_name: str, user_name: str
    ) -> GroupUserDeleteResponse:
        """
        Remove a user from a group.

        :param group_name: Name of the group.
        :param user_name: Name of the user.

        :returns: Response containing the group name and user name.
        """
        response = requests.delete(
            f"{self.base_url}/v1/group/{group_name}/user/{user_name}",
            headers=self.headers,
        )
        self.raise_for_status(response)
        return GroupUserDeleteResponse.model_validate(response.json())

    def update_role(
        self, *, role_name: str, policies: list[PolicyRequest]
    ) -> RoleCreateResponse:
        """
        Update the policies of a role.

        :param role_name: Name of the role.
        :param policies: List of policies to be updated.

        :returns: Response containing the updated role and its new policies.
        """
        response = requests.put(
            f"{self.base_url}/v1/roles/{role_name}",
            json=[json.loads(policy.model_dump_json()) for policy in policies],
            headers=self.headers,
        )
        self.raise_for_status(response)
        return RoleCreateResponse.model_validate(response.json())

    @staticmethod
    def raise_for_status(response: requests.Response):
        """
        Raise an exception if the response status code is not in the 200 range.

        :param response: Response object from the request.

        :raises HTTPError: If the response status code is not in the 200 range.
        """
        http_error_msg = ""
        if isinstance(response.reason, bytes):
            # We attempt to decode utf-8 first because some servers
            # choose to localize their reason strings. If the string
            # isn't utf-8, we fall back to iso-8859-1 for all other
            # encodings. (See PR #3538)
            try:
                reason = response.reason.decode("utf-8")
            except UnicodeDecodeError:
                reason = response.reason.decode("iso-8859-1")
        else:
            reason = response.content

        if 400 <= response.status_code < 500:
            http_error_msg = (
                f"{response.status_code} Client Error: {reason} for url: {response.url}"
            )

        elif 500 <= response.status_code < 600:
            http_error_msg = (
                f"{response.status_code} Server Error: {reason} for url: {response.url}"
            )

        if http_error_msg:
            raise HTTPError(http_error_msg, response=response)
