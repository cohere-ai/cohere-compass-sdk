import json
from typing import Dict, List, Type, TypeVar

import requests
from pydantic import BaseModel
from requests import HTTPError

from compass_sdk.models.rbac import (
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
    def __init__(self, compass_url: str, root_user_token: str):
        self.base_url = compass_url + "/api/security/admin/rbac"
        self.headers = {
            "Authorization": f"Bearer {root_user_token}",
            "Content-Type": "application/json",
        }

    T = TypeVar("T", bound=BaseModel)
    U = TypeVar("U", bound=BaseModel)
    Headers = Dict[str, str]

    @staticmethod
    def _fetch_entities(url: str, headers: Headers, entity_type: Type[T]) -> List[T]:
        response = requests.get(url, headers=headers)
        CompassRootClient.raise_for_status(response)
        return [entity_type.model_validate(entity) for entity in response.json()]

    @staticmethod
    def _create_entities(
        url: str, headers: Headers, entity_request: List[T], entity_response: Type[U]
    ) -> List[U]:
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
        url: str, headers: Headers, names: List[str], entity_response: Type[U]
    ) -> List[U]:
        entities = ",".join(names)
        response = requests.delete(f"{url}/{entities}", headers=headers)
        CompassRootClient.raise_for_status(response)
        return [entity_response.model_validate(entity) for entity in response.json()]

    def fetch_users(self) -> List[UserFetchResponse]:
        return self._fetch_entities(
            f"{self.base_url}/v1/users", self.headers, UserFetchResponse
        )

    def fetch_groups(self) -> List[GroupFetchResponse]:
        return self._fetch_entities(
            f"{self.base_url}/v1/groups", self.headers, GroupFetchResponse
        )

    def fetch_roles(self) -> List[RoleFetchResponse]:
        return self._fetch_entities(
            f"{self.base_url}/v1/roles", self.headers, RoleFetchResponse
        )

    def fetch_role_mappings(self) -> List[RoleMappingResponse]:
        return self._fetch_entities(
            f"{self.base_url}/v1/role-mappings", self.headers, RoleMappingResponse
        )

    def create_users(
        self, *, users: List[UserCreateRequest]
    ) -> List[UserCreateResponse]:
        return self._create_entities(
            url=f"{self.base_url}/v1/users",
            headers=self.headers,
            entity_request=users,
            entity_response=UserCreateResponse,
        )

    def create_groups(
        self, *, groups: List[GroupCreateRequest]
    ) -> List[GroupCreateResponse]:
        return self._create_entities(
            url=f"{self.base_url}/v1/groups",
            headers=self.headers,
            entity_request=groups,
            entity_response=GroupCreateResponse,
        )

    def create_roles(
        self, *, roles: List[RoleCreateRequest]
    ) -> List[RoleCreateResponse]:
        return self._create_entities(
            url=f"{self.base_url}/v1/roles",
            headers=self.headers,
            entity_request=roles,
            entity_response=RoleCreateResponse,
        )

    def create_role_mappings(
        self, *, role_mappings: List[RoleMappingRequest]
    ) -> List[RoleMappingResponse]:
        return self._create_entities(
            url=f"{self.base_url}/v1/role-mappings",
            headers=self.headers,
            entity_request=role_mappings,
            entity_response=RoleMappingResponse,
        )

    def delete_users(self, *, user_names: List[str]) -> List[UserDeleteResponse]:
        return self._delete_entities(
            f"{self.base_url}/v1/users", self.headers, user_names, UserDeleteResponse
        )

    def delete_groups(self, *, group_names: List[str]) -> List[GroupDeleteResponse]:
        return self._delete_entities(
            f"{self.base_url}/v1/groups", self.headers, group_names, GroupDeleteResponse
        )

    def delete_roles(self, *, role_ids: List[str]) -> List[RoleDeleteResponse]:
        return self._delete_entities(
            f"{self.base_url}/v1/roles", self.headers, role_ids, RoleDeleteResponse
        )

    def delete_role_mappings(
        self, *, role_name: str, group_name: str
    ) -> List[RoleMappingDeleteResponse]:
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
        response = requests.delete(
            f"{self.base_url}/v1/group/{group_name}/user/{user_name}",
            headers=self.headers,
        )
        self.raise_for_status(response)
        return GroupUserDeleteResponse.model_validate(response.json())

    def update_role(
        self, *, role_name: str, policies: List[PolicyRequest]
    ) -> RoleCreateResponse:
        response = requests.put(
            f"{self.base_url}/v1/roles/{role_name}",
            json=[json.loads(policy.model_dump_json()) for policy in policies],
            headers=self.headers,
        )
        self.raise_for_status(response)
        return RoleCreateResponse.model_validate(response.json())

    @staticmethod
    def raise_for_status(response: requests.Response):
        """Raises :class:`HTTPError`, if one occurred."""

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
