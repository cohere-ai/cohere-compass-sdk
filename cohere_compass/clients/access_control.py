"""
Access control client for Compass RBAC operations.

This module provides the CompassRootClient for managing users, groups, roles, and
permissions in the Compass RBAC system.
"""

import json
from datetime import timedelta
from typing import TypeVar

import httpx
from pydantic import BaseModel

from cohere_compass.constants import DEFAULT_ROOT_CLIENT_TIMEOUT
from cohere_compass.exceptions import handle_httpx_exceptions
from cohere_compass.models.access_control import (
    DetailedGroup,
    DetailedRole,
    DetailedUser,
    Group,
    GroupMembership,
    GroupRole,
    GroupsPage,
    PageDirection,
    PageInfo,
    Role,
    RolesPage,
    User,
    UsersPage,
    UserWithToken,
)


class CompassRootClient:
    """Client for interacting with Compass RBAC API V2 as a root user."""

    def __init__(
        self,
        compass_url: str,
        root_user_token: str,
        timeout: timedelta | None = None,
    ):
        """
        Initialize a new CompassRootClient.

        :param compass_url: URL of the Compass instance.
        :param root_user_token: Root user token for Compass instance.
        :param timeout: Request timeout duration. Defaults to
            DEFAULT_ROOT_CLIENT_TIMEOUT (5 seconds).
        """
        self.base_url = compass_url + "/security/admin/rbac"
        self.headers = {
            "Authorization": f"Bearer {root_user_token}",
            "Content-Type": "application/json",
        }
        self.timeout = timeout or DEFAULT_ROOT_CLIENT_TIMEOUT

    T = TypeVar("T", bound=BaseModel)
    U = TypeVar("U", bound=BaseModel)
    Headers = dict[str, str]

    @staticmethod
    def _fetch_page(
        url: str,
        headers: Headers,
        entity_response: type[T],
        *,
        filter: str | None = None,
        page_info: PageInfo | None = None,
        direction: PageDirection | None = PageDirection.NEXT,
        timeout: float,
    ) -> T:
        params: dict[str, str] = {}
        if filter is not None:
            params["filter"] = filter

        if page_info:
            cursor_value = page_info.next if direction == PageDirection.NEXT else page_info.previous
            if cursor_value is not None:
                params["cursor"] = cursor_value
            if page_info.filter is not None:
                params["filter"] = page_info.filter

        with handle_httpx_exceptions():
            response = httpx.get(url, headers=headers, params=params, timeout=timeout)
            response.raise_for_status()
        return entity_response.model_validate(response.json())

    @staticmethod
    def _fetch_entity(url: str, headers: Headers, entity_response: type[T], entity_name: str, timeout: float) -> T:
        with handle_httpx_exceptions():
            response = httpx.get(f"{url}/{entity_name}", headers=headers, timeout=timeout)
            response.raise_for_status()
        return entity_response.model_validate(response.json())

    @staticmethod
    def _create_entities(
        url: str, headers: Headers, entity_request: list[T], entity_response: type[U], timeout: float
    ) -> list[U]:
        with handle_httpx_exceptions():
            response = httpx.post(
                url,
                json=[json.loads(entity.model_dump_json()) for entity in entity_request],
                headers=headers,
                timeout=timeout,
            )
            response.raise_for_status()
        return [entity_response.model_validate(response) for response in response.json()]

    @staticmethod
    def _update_entity(
        url: str,
        headers: Headers,
        entity_name: str,
        entity: BaseModel,
        entity_response: type[U],
        timeout: float,
    ) -> U:
        with handle_httpx_exceptions():
            response = httpx.put(
                f"{url}/{entity_name}",
                json=json.loads(entity.model_dump_json()),
                headers=headers,
                timeout=timeout,
            )
            response.raise_for_status()
        return entity_response.model_validate(response.json())

    @staticmethod
    def _delete_entities(
        url: str, headers: Headers, names: list[str], entity_response: type[U], timeout: float
    ) -> list[U]:
        entities = ",".join(names)
        with handle_httpx_exceptions():
            response = httpx.delete(f"{url}/{entities}", headers=headers, timeout=timeout)
            response.raise_for_status()
        return [entity_response.model_validate(entity) for entity in response.json()]

    def get_users_page(
        self,
        *,
        filter: str | None = None,
        page_info: PageInfo | None = None,
        direction: PageDirection | None = None,
    ) -> UsersPage:
        """
        Fetch a page of Users. Defaults to fetching the first page.

        :param filter: Optional filter to apply to the user set.
        :param page_info: Optional pagination information.
        :param direction: Optional direction to paginate in. Defaults to NEXT.

        Important notes:

        When fetching the first page, the `page_info` parameter
        should be `None`. When fetching subsequent pages, the `page_info` parameter
        should be the `page_info` from the existing page.

        If filter and page_info.filter are both None, all Users are eligible to be
        fetched. If both are provided, the page_info.filter is used.

        :return: Page of Users.
        """
        return self._fetch_page(
            url=f"{self.base_url}/v2/users",
            headers=self.headers,
            entity_response=UsersPage,
            filter=filter,
            page_info=page_info,
            direction=direction,
            timeout=self.timeout.total_seconds(),
        )

    def create_users(self, users: list[User]) -> list[UserWithToken]:
        """
        Create new Users.

        :param users: List of Users to create.

        :return: List of created UserWithToken.
        """
        return self._create_entities(
            url=f"{self.base_url}/v2/users",
            headers=self.headers,
            entity_request=users,
            entity_response=UserWithToken,
            timeout=self.timeout.total_seconds(),
        )

    def get_detailed_user(self, user_name: str) -> DetailedUser:
        """
        Get a detailed User.

        :param user_name: Name of the User to get.

        :return: DetailedUser containing the User and a Page of Groups plus PageInfo.
        """
        return self._fetch_entity(
            url=f"{self.base_url}/v2/users",
            headers=self.headers,
            entity_response=DetailedUser,
            entity_name=user_name,
            timeout=self.timeout.total_seconds(),
        )

    def get_user_groups_page(
        self,
        user_name: str,
        *,
        filter: str | None = None,
        page_info: PageInfo | None = None,
        direction: PageDirection | None = None,
    ) -> GroupsPage:
        """
        Fetch a page of Groups a User is a Member of.

        Defaults to fetching the first page.

        :param user_name: Name of the User to fetch Groups for.
        :param filter: Optional filter to apply to the Group set.
        :param page_info: Optional pagination information.
        :param direction: Optional direction to paginate in. Defaults to NEXT.

        Important notes:

        When fetching the first page, the `page_info` parameter
        should be `None`. When fetching subsequent pages, the `page_info` parameter
        should be the `page_info` from the existing page.

        If filter and page_info.filter are both None, all Groups are eligible to be
        fetched. If both are provided, the page_info.filter is used.

        :return: Page of Groups for the User.
        """
        return self._fetch_page(
            url=f"{self.base_url}/v2/users/{user_name}/groups",
            headers=self.headers,
            entity_response=GroupsPage,
            filter=filter,
            page_info=page_info,
            direction=direction,
            timeout=self.timeout.total_seconds(),
        )

    def delete_users(self, user_names: list[str]) -> list[User]:
        """
        Delete Users.

        :param user_names: List of User names to delete.

        :return: List of deleted Users.
        """
        return self._delete_entities(
            url=f"{self.base_url}/v2/users",
            headers=self.headers,
            names=user_names,
            entity_response=User,
            timeout=self.timeout.total_seconds(),
        )

    def get_roles_page(
        self,
        *,
        filter: str | None = None,
        page_info: PageInfo | None = None,
        direction: PageDirection | None = None,
    ) -> RolesPage:
        """
        Fetch a page of Roles. Defaults to fetching the first page.

        :param filter: Optional filter to apply to the role set.
        :param page_info: Optional pagination information.
        :param direction: Optional direction to paginate in. Defaults to NEXT.

        Important notes:

        When fetching the first page, the `page_info` parameter
        should be `None`. When fetching subsequent pages, the `page_info` parameter
        should be the `page_info` from the existing page.

        If filter and page_info.filter are both None, all Roles are eligible to be
        fetched. If both are provided, the page_info.filter is used.

        :return: Page of Roles.
        """
        return self._fetch_page(
            url=f"{self.base_url}/v2/roles",
            headers=self.headers,
            entity_response=RolesPage,
            filter=filter,
            page_info=page_info,
            direction=direction,
            timeout=self.timeout.total_seconds(),
        )

    def create_roles(self, roles: list[Role]) -> list[Role]:
        """
        Create new Roles.

        :param roles: List of Roles to create.

        :return: List of created Roles.
        """
        return self._create_entities(
            url=f"{self.base_url}/v2/roles",
            headers=self.headers,
            entity_request=roles,
            entity_response=Role,
            timeout=self.timeout.total_seconds(),
        )

    def get_detailed_role(self, role_name: str) -> DetailedRole:
        """
        Get a detailed Role.

        :param role_name: Name of the Role to get.

        :return: DetailedRole containing the Role and a Page of Groups plus PageInfo.
        """
        return self._fetch_entity(
            url=f"{self.base_url}/v2/roles",
            headers=self.headers,
            entity_response=DetailedRole,
            entity_name=role_name,
            timeout=self.timeout.total_seconds(),
        )

    def get_role_groups_page(
        self,
        role_name: str,
        *,
        filter: str | None = None,
        page_info: PageInfo | None = None,
        direction: PageDirection | None = None,
    ) -> GroupsPage:
        """
        Fetch a page of Groups for a Role. Defaults to fetching the first page.

        :param role_name: Name of the Role to fetch Groups for.
        :param filter: Optional filter to apply to the Group set.
        :param page_info: Optional pagination information.
        :param direction: Optional direction to paginate in. Defaults to NEXT.

        Important notes:

        When fetching the first page, the `page_info` parameter
        should be `None`. When fetching subsequent pages, the `page_info` parameter
        should be the `page_info` from the existing page.

        If filter and page_info.filter are both None, all Groups are eligible to be
        fetched. If both are provided, the page_info.filter is used.

        :return: Page of Groups for the Role.
        """
        return self._fetch_page(
            url=f"{self.base_url}/v2/roles/{role_name}/groups",
            headers=self.headers,
            entity_response=GroupsPage,
            filter=filter,
            page_info=page_info,
            direction=direction,
            timeout=self.timeout.total_seconds(),
        )

    def update_role(self, role: Role) -> Role:
        """
        Update a Role.

        :param role: Role to update.

        :return: Updated Role.
        """
        with handle_httpx_exceptions():
            response = httpx.put(
                f"{self.base_url}/v2/roles/{role.role_name}",
                json=[json.loads(entity.model_dump_json()) for entity in role.policies],
                headers=self.headers,
                timeout=self.timeout.total_seconds(),
            )
            response.raise_for_status()
        return Role.model_validate(response.json())

    def delete_roles(self, role_names: list[str]) -> list[Role]:
        """
        Delete Roles.

        :param role_names: List of Role names to delete.

        :return: List of deleted Roles.
        """
        return self._delete_entities(
            url=f"{self.base_url}/v2/roles",
            headers=self.headers,
            names=role_names,
            entity_response=Role,
            timeout=self.timeout.total_seconds(),
        )

    def get_groups_page(
        self,
        *,
        filter: str | None = None,
        page_info: PageInfo | None = None,
        direction: PageDirection | None = None,
    ) -> GroupsPage:
        """
        Fetch a page of Groups. Defaults to fetching the first page.

        :param filter: Optional filter to apply to the Group set.
        :param page_info: Optional pagination information.
        :param direction: Optional direction to paginate in. Defaults to NEXT.

        Important notes:

        When fetching the first page, the `page_info` parameter
        should be `None`. When fetching subsequent pages, the `page_info` parameter
        should be the `page_info` from the existing page.

        If filter and page_info.filter are both None, all Groups are eligible to be
        fetched. If both are provided, the page_info.filter is used.

        :return: Page of Groups.
        """
        return self._fetch_page(
            url=f"{self.base_url}/v2/groups",
            headers=self.headers,
            entity_response=GroupsPage,
            filter=filter,
            page_info=page_info,
            direction=direction,
            timeout=self.timeout.total_seconds(),
        )

    def create_groups(self, groups: list[Group]) -> list[Group]:
        """
        Create new Groups.

        :param groups: List of Groups to create.

        :return: List of created Groups.
        """
        return self._create_entities(
            url=f"{self.base_url}/v2/groups",
            headers=self.headers,
            entity_request=groups,
            entity_response=Group,
            timeout=self.timeout.total_seconds(),
        )

    def get_detailed_group(self, group_name: str) -> DetailedGroup:
        """
        Get a detailed Group.

        :param group_name: Name of the Group to get.

        :return: DetailedGroup containing the Group and:
            * a Page of Users plus PageInfo
            * a Page of Roles plus PageInfo.
        """
        return self._fetch_entity(
            url=f"{self.base_url}/v2/groups",
            headers=self.headers,
            entity_response=DetailedGroup,
            entity_name=group_name,
            timeout=self.timeout.total_seconds(),
        )

    def delete_groups(self, group_names: list[str]) -> list[Group]:
        """
        Delete Groups.

        :param group_names: List of Group names to delete.

        :return: List of deleted Groups.
        """
        return self._delete_entities(
            url=f"{self.base_url}/v2/groups",
            headers=self.headers,
            names=group_names,
            entity_response=Group,
            timeout=self.timeout.total_seconds(),
        )

    def add_members_to_group(self, group_name: str, user_names: list[str]) -> list[GroupMembership]:
        """
        Add Users to a Group.

        :param group_name: Name of the Group to add Users to.
        :param user_names: List of User names added to the Group.

        :return: List of added GroupMemberships.
        """
        with handle_httpx_exceptions():
            response = httpx.post(
                f"{self.base_url}/v2/groups/{group_name}/users",
                json=[{"user_name": user_name} for user_name in user_names],
                headers=self.headers,
                timeout=self.timeout.total_seconds(),
            )
            response.raise_for_status()
        return [GroupMembership.model_validate(member) for member in response.json()]

    def remove_members_from_group(self, group_name: str, user_names: list[str]) -> list[GroupMembership]:
        """
        Remove Users from a Group.

        :param group_name: Name of the Group to remove Users from.
        :param user_names: List of User names removed from the Group.

        :return: List of removed GroupMemberships.
        """
        return self._delete_entities(
            url=f"{self.base_url}/v2/groups/{group_name}/users",
            headers=self.headers,
            names=user_names,
            entity_response=GroupMembership,
            timeout=self.timeout.total_seconds(),
        )

    def get_group_members_page(
        self,
        group_name: str,
        *,
        filter: str | None = None,
        page_info: PageInfo | None = None,
        direction: PageDirection | None = None,
    ) -> UsersPage:
        """
        Fetch a page of Users in a Group. Defaults to fetching the first page.

        :param group_name: Name of the Group to fetch Users for.
        :param filter: Optional filter to apply to the User set.
        :param page_info: Optional pagination information.
        :param direction: Optional direction to paginate in. Defaults to NEXT.

        Important notes:

        When fetching the first page, the `page_info` parameter
        should be `None`. When fetching subsequent pages, the `page_info` parameter
        should be the `page_info` from the existing page.

        If filter and page_info.filter are both None, all Users are eligible to be
        fetched. If both are provided, the page_info.filter is used.

        :return: Page of Users in the Group.
        """
        return self._fetch_page(
            url=f"{self.base_url}/v2/groups/{group_name}/users",
            headers=self.headers,
            entity_response=UsersPage,
            filter=filter,
            page_info=page_info,
            direction=direction,
            timeout=self.timeout.total_seconds(),
        )

    def add_roles_to_group(self, group_name: str, role_names: list[str]) -> list[GroupRole]:
        """
        Add Roles to a Group.

        :param group_name: Name of the Group to add Roles to.
        :param role_names: List of Role names added to the Group.

        :return: List of added GroupRoles.
        """
        with handle_httpx_exceptions():
            response = httpx.post(
                f"{self.base_url}/v2/groups/{group_name}/roles",
                json=[{"role_name": role_name} for role_name in role_names],
                headers=self.headers,
                timeout=self.timeout.total_seconds(),
            )
            response.raise_for_status()
        return [GroupRole.model_validate(member) for member in response.json()]

    def remove_roles_from_group(self, group_name: str, role_names: list[str]) -> list[GroupRole]:
        """
        Remove Roles from a Group.

        :param group_name: Name of the Group to remove Roles from.
        :param role_names: List of Role names removed from the Group.

        :return: List of removed GroupRoles.
        """
        return self._delete_entities(
            url=f"{self.base_url}/v2/groups/{group_name}/roles",
            headers=self.headers,
            names=role_names,
            entity_response=GroupRole,
            timeout=self.timeout.total_seconds(),
        )

    def get_group_roles_page(
        self,
        group_name: str,
        *,
        filter: str | None = None,
        page_info: PageInfo | None = None,
        direction: PageDirection | None = None,
    ) -> RolesPage:
        """
        Fetch a page of Roles in a Group. Defaults to fetching the first page.

        :param group_name: Name of the Group to fetch Roles for.
        :param filter: Optional filter to apply to the Role set.
        :param page_info: Optional pagination information.
        :param direction: Optional direction to paginate in. Defaults to NEXT.

        Important notes:

        When fetching the first page, the `page_info` parameter
        should be `None`. When fetching subsequent pages, the `page_info` parameter
        should be the `page_info` from the existing page.

        If filter and page_info.filter are both None, all Roles are eligible to be
        fetched. If both are provided, the page_info.filter is used.

        :return: Page of Roles in the Group.
        """
        return self._fetch_page(
            url=f"{self.base_url}/v2/groups/{group_name}/roles",
            headers=self.headers,
            entity_response=RolesPage,
            filter=filter,
            page_info=page_info,
            direction=direction,
            timeout=self.timeout.total_seconds(),
        )
