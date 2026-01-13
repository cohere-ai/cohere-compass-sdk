from typing import Any

import httpx
import pytest
import respx
from respx import MockRouter

from cohere_compass.clients.access_control import CompassRootClient
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
    Permission,
    Policy,
    Role,
    RolesPage,
    User,
    UsersPage,
    UserWithToken,
)


@pytest.fixture
def root_client() -> CompassRootClient:
    return CompassRootClient(
        compass_url="http://test.com",
        root_user_token="test_token",
    )


@pytest.fixture
def mock_users_page() -> dict[str, Any]:
    return {
        "page_info": {
            "next": "next_cursor",
            "previous": None,
            "filter": None,
            "total": 2,
            "page": 1,
            "total_pages": 1,
            "page_size": 10,
        },
        "users": [
            {
                "user_name": "user1",
                "email": "user1@example.com",
                "display_name": "User One",
            },
            {
                "user_name": "user2",
                "email": "user2@example.com",
                "display_name": "User Two",
            },
        ],
    }


@pytest.fixture
def mock_roles_page() -> dict[str, Any]:
    return {
        "page_info": {
            "next": "next_cursor",
            "previous": None,
            "filter": None,
            "total": 2,
            "page": 1,
            "total_pages": 1,
            "page_size": 10,
        },
        "roles": [
            {
                "role_name": "admin",
                "policies": [
                    {
                        "indexes": ["*"],
                        "permission": "write",
                    }
                ],
            },
            {
                "role_name": "reader",
                "policies": [
                    {
                        "indexes": ["*"],
                        "permission": "read",
                    }
                ],
            },
        ],
    }


@pytest.fixture
def mock_groups_page() -> dict[str, Any]:
    return {
        "page_info": {
            "next": None,
            "previous": None,
            "filter": None,
            "total": 2,
            "page": 1,
            "total_pages": 1,
            "page_size": 10,
        },
        "groups": [
            {
                "group_name": "developers",
                "display_name": "Development Team",
            },
            {
                "group_name": "analysts",
                "display_name": "Analytics Team",
            },
        ],
    }


class TestCompassRootClient:
    @respx.mock
    def test_get_users_page(
        self,
        root_client: CompassRootClient,
        mock_users_page: dict[str, Any],
        respx_mock: MockRouter,
    ) -> None:
        respx_mock.get("http://test.com/security/admin/rbac/v2/users").mock(
            return_value=httpx.Response(200, json=mock_users_page)
        )

        result = root_client.get_users_page()

        assert isinstance(result, UsersPage)
        assert len(result.users) == 2
        assert result.users[0].user_name == "user1"
        assert result.page_info.next == "next_cursor"

    @respx.mock
    def test_get_users_page_with_filter(
        self,
        root_client: CompassRootClient,
        mock_users_page: dict[str, Any],
        respx_mock: MockRouter,
    ) -> None:
        respx_mock.get(
            "http://test.com/security/admin/rbac/v2/users",
            params={"filter": "email:user1*"},
        ).mock(return_value=httpx.Response(200, json=mock_users_page))

        result = root_client.get_users_page(filter="email:user1*")

        assert isinstance(result, UsersPage)

    @respx.mock
    def test_get_users_page_with_pagination(
        self,
        root_client: CompassRootClient,
        mock_users_page: dict[str, Any],
        respx_mock: MockRouter,
    ) -> None:
        page_info = PageInfo(
            next="cursor123",
            previous=None,
            filter=None,
            total=10,
            page=1,
            total_pages=1,
            page_size=10,
        )
        respx_mock.get(
            "http://test.com/security/admin/rbac/v2/users",
            params={"cursor": "cursor123"},
        ).mock(return_value=httpx.Response(200, json=mock_users_page))

        result = root_client.get_users_page(page_info=page_info, direction=PageDirection.NEXT)

        assert isinstance(result, UsersPage)

    @respx.mock
    def test_create_users(self, root_client: CompassRootClient, respx_mock: MockRouter) -> None:
        users = [
            User(user_name="newuser1"),
            User(user_name="newuser2"),
        ]

        mock_response = [
            {
                "user_name": "newuser1",
                "email": "newuser1@example.com",
                "display_name": "New User 1",
                "token": "token1",
            },
            {
                "user_name": "newuser2",
                "email": "newuser2@example.com",
                "display_name": "New User 2",
                "token": "token2",
            },
        ]

        respx_mock.post("http://test.com/security/admin/rbac/v2/users").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = root_client.create_users(users)

        assert len(result) == 2
        assert all(isinstance(u, UserWithToken) for u in result)
        assert result[0].token == "token1"
        assert result[1].token == "token2"

    @respx.mock
    def test_get_detailed_user(self, root_client: CompassRootClient, respx_mock: MockRouter) -> None:
        mock_response = {
            "user_name": "user1",
            "groups": [
                {"group_name": "developers"},
                {"group_name": "analysts"},
            ],
            "groups_page_info": {
                "total": 2,
                "page": 1,
                "filter": "test-filter",
                "total_pages": 3,
                "next": "next_cursor",
                "previous": "previous_cursor",
                "page_size": 10,
            },
        }

        respx_mock.get("http://test.com/security/admin/rbac/v2/users/user1").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = root_client.get_detailed_user("user1")

        assert isinstance(result, DetailedUser)
        assert result.user_name == "user1"
        assert len(result.groups) == 2
        assert result.groups[0].group_name == "developers"
        assert result.groups[1].group_name == "analysts"
        assert result.groups_page_info.next == "next_cursor"
        assert result.groups_page_info.previous == "previous_cursor"
        assert result.groups_page_info.filter == "test-filter"
        assert result.groups_page_info.total == 2
        assert result.groups_page_info.page == 1
        assert result.groups_page_info.total_pages == 3
        assert result.groups_page_info.page_size == 10

    @respx.mock
    def test_get_user_groups_page(
        self,
        root_client: CompassRootClient,
        mock_groups_page: dict[str, Any],
        respx_mock: MockRouter,
    ) -> None:
        respx_mock.get("http://test.com/security/admin/rbac/v2/users/user1/groups").mock(
            return_value=httpx.Response(200, json=mock_groups_page)
        )

        result = root_client.get_user_groups_page("user1")

        assert isinstance(result, GroupsPage)
        assert len(result.groups) == 2

    @respx.mock
    def test_delete_users(self, root_client: CompassRootClient, respx_mock: MockRouter) -> None:
        mock_response = [
            {
                "user_name": "user1",
                "email": "user1@example.com",
                "display_name": "User One",
            },
            {
                "user_name": "user2",
                "email": "user2@example.com",
                "display_name": "User Two",
            },
        ]

        respx_mock.delete("http://test.com/security/admin/rbac/v2/users/user1,user2").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = root_client.delete_users(["user1", "user2"])

        assert len(result) == 2
        assert all(isinstance(u, User) for u in result)
        assert result[0].user_name == "user1"

    @respx.mock
    def test_get_roles_page(
        self,
        root_client: CompassRootClient,
        mock_roles_page: dict[str, Any],
        respx_mock: MockRouter,
    ) -> None:
        respx_mock.get("http://test.com/security/admin/rbac/v2/roles").mock(
            return_value=httpx.Response(200, json=mock_roles_page)
        )

        result = root_client.get_roles_page()

        assert isinstance(result, RolesPage)
        assert len(result.roles) == 2
        assert result.roles[0].role_name == "admin"

    @respx.mock
    def test_create_roles(self, root_client: CompassRootClient, respx_mock: MockRouter) -> None:
        roles = [
            Role(
                role_name="editor",
                policies=[
                    Policy(
                        indexes=["*"],
                        permission=Permission.READ,
                    )
                ],
            )
        ]

        mock_response = [
            {
                "role_name": "editor",
                "policies": [
                    {
                        "indexes": ["*"],
                        "permission": "read",
                    }
                ],
            }
        ]

        respx_mock.post("http://test.com/security/admin/rbac/v2/roles").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = root_client.create_roles(roles)

        assert len(result) == 1
        assert isinstance(result[0], Role)
        assert result[0].role_name == "editor"

    @respx.mock
    def test_get_detailed_role(self, root_client: CompassRootClient, respx_mock: MockRouter) -> None:
        mock_response = {
            "role_name": "admin",
            "policies": [
                {
                    "indexes": ["*"],
                    "permission": "read",
                }
            ],
            "groups": [{"group_name": "admins"}],
            "groups_page_info": {
                "next": None,
                "previous": None,
                "filter": None,
                "total": 1,
                "page": 1,
                "total_pages": 1,
                "page_size": 10,
            },
        }

        respx_mock.get("http://test.com/security/admin/rbac/v2/roles/admin").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = root_client.get_detailed_role("admin")

        assert isinstance(result, DetailedRole)
        assert result.role_name == "admin"
        assert len(result.groups) == 1
        assert result.groups[0].group_name == "admins"
        assert len(result.policies) == 1
        assert result.policies[0].indexes == ["*"]
        assert result.policies[0].permission == Permission.READ
        assert result.groups_page_info.next is None
        assert result.groups_page_info.previous is None
        assert result.groups_page_info.filter is None
        assert result.groups_page_info.total == 1
        assert result.groups_page_info.page == 1
        assert result.groups_page_info.total_pages == 1
        assert result.groups_page_info.page_size == 10

    @respx.mock
    def test_update_role(self, root_client: CompassRootClient, respx_mock: MockRouter) -> None:
        role = Role(
            role_name="editor",
            policies=[
                Policy(
                    indexes=["my-test-index"],
                    permission=Permission.READ,
                )
            ],
        )

        mock_response = {
            "role_name": "editor",
            "policies": [
                {
                    "indexes": ["my-test-index"],
                    "permission": "read",
                }
            ],
        }

        respx_mock.put("http://test.com/security/admin/rbac/v2/roles/editor").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = root_client.update_role(role)

        assert isinstance(result, Role)
        assert result.role_name == "editor"
        assert result.policies[0].indexes == ["my-test-index"]
        assert result.policies[0].permission == Permission.READ

    @respx.mock
    def test_delete_roles(self, root_client: CompassRootClient, respx_mock: MockRouter) -> None:
        mock_response: list[dict[str, Any]] = [
            {
                "role_name": "role1",
                "policies": [],
            },
            {
                "role_name": "role2",
                "policies": [],
            },
        ]

        respx_mock.delete("http://test.com/security/admin/rbac/v2/roles/role1,role2").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = root_client.delete_roles(["role1", "role2"])

        assert len(result) == 2
        assert all(isinstance(r, Role) for r in result)

    @respx.mock
    def test_get_groups_page(
        self,
        root_client: CompassRootClient,
        mock_groups_page: dict[str, Any],
        respx_mock: MockRouter,
    ) -> None:
        respx_mock.get("http://test.com/security/admin/rbac/v2/groups").mock(
            return_value=httpx.Response(200, json=mock_groups_page)
        )

        result = root_client.get_groups_page()

        assert isinstance(result, GroupsPage)
        assert len(result.groups) == 2
        assert result.groups[0].group_name == "developers"

    @respx.mock
    def test_create_groups(self, root_client: CompassRootClient, respx_mock: MockRouter) -> None:
        groups = [
            Group(
                group_name="testers",
            )
        ]

        mock_response = [
            {
                "group_name": "testers",
                "display_name": "Testing Team",
            }
        ]

        respx_mock.post("http://test.com/security/admin/rbac/v2/groups").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = root_client.create_groups(groups)

        assert len(result) == 1
        assert isinstance(result[0], Group)
        assert result[0].group_name == "testers"

    @respx.mock
    def test_get_detailed_group(self, root_client: CompassRootClient, respx_mock: MockRouter) -> None:
        mock_response = {
            "group_name": "developers",
            "roles": [
                {
                    "role_name": "developer",
                    "policies": [
                        {
                            "indexes": ["*"],
                            "permission": "read",
                        },
                        {
                            "indexes": ["writable-*"],
                            "permission": "write",
                        },
                    ],
                },
            ],
            "roles_page_info": {
                "total": 2,
                "page": 1,
                "filter": "test-filter",
                "total_pages": 3,
                "next": "next_cursor",
                "previous": "previous_cursor",
                "page_size": 10,
            },
            "users": [
                {"user_name": "user1"},
                {"user_name": "user2"},
            ],
            "users_page_info": {
                "total": 2,
                "page": 1,
                "filter": "test-filter",
                "total_pages": 3,
                "next": "next_cursor",
                "previous": "previous_cursor",
                "page_size": 10,
            },
        }

        respx_mock.get("http://test.com/security/admin/rbac/v2/groups/developers").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = root_client.get_detailed_group("developers")

        assert isinstance(result, DetailedGroup)
        assert result.group_name == "developers"
        assert len(result.users) == 2
        assert result.users[0].user_name == "user1"
        assert result.users[1].user_name == "user2"
        assert result.users_page_info.next == "next_cursor"
        assert result.users_page_info.previous == "previous_cursor"
        assert result.users_page_info.filter == "test-filter"
        assert result.users_page_info.total == 2
        assert result.users_page_info.page == 1
        assert result.users_page_info.total_pages == 3
        assert result.users_page_info.page_size == 10
        assert len(result.roles) == 1
        assert result.roles[0].role_name == "developer"
        assert result.roles_page_info.next == "next_cursor"
        assert result.roles_page_info.previous == "previous_cursor"
        assert result.roles_page_info.filter == "test-filter"
        assert result.roles_page_info.total == 2
        assert result.roles_page_info.page == 1
        assert result.roles_page_info.total_pages == 3
        assert result.roles_page_info.page_size == 10

    @respx.mock
    def test_delete_groups(self, root_client: CompassRootClient, respx_mock: MockRouter) -> None:
        mock_response = [
            {
                "group_name": "group1",
                "display_name": "Group 1",
            },
            {
                "group_name": "group2",
                "display_name": "Group 2",
            },
        ]

        respx_mock.delete("http://test.com/security/admin/rbac/v2/groups/group1,group2").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = root_client.delete_groups(["group1", "group2"])

        assert len(result) == 2
        assert all(isinstance(g, Group) for g in result)

    @respx.mock
    def test_add_members_to_group(self, root_client: CompassRootClient, respx_mock: MockRouter) -> None:
        mock_response = [
            {
                "user_name": "user1",
                "group_name": "developers",
            },
            {
                "user_name": "user2",
                "group_name": "developers",
            },
        ]

        respx_mock.post("http://test.com/security/admin/rbac/v2/groups/developers/users").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = root_client.add_members_to_group("developers", ["user1", "user2"])

        assert len(result) == 2
        assert all(isinstance(m, GroupMembership) for m in result)
        assert result[0].user_name == "user1"
        assert result[0].group_name == "developers"

    @respx.mock
    def test_remove_members_from_group(self, root_client: CompassRootClient, respx_mock: MockRouter) -> None:
        mock_response = [
            {
                "user_name": "user1",
                "group_name": "developers",
            },
            {
                "user_name": "user2",
                "group_name": "developers",
            },
        ]

        respx_mock.delete("http://test.com/security/admin/rbac/v2/groups/developers/users/user1,user2").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = root_client.remove_members_from_group("developers", ["user1", "user2"])

        assert len(result) == 2
        assert all(isinstance(m, GroupMembership) for m in result)

    @respx.mock
    def test_get_group_members_page(
        self,
        root_client: CompassRootClient,
        mock_users_page: dict[str, Any],
        respx_mock: MockRouter,
    ) -> None:
        respx_mock.get("http://test.com/security/admin/rbac/v2/groups/developers/users").mock(
            return_value=httpx.Response(200, json=mock_users_page)
        )

        result = root_client.get_group_members_page("developers")

        assert isinstance(result, UsersPage)
        assert len(result.users) == 2

    @respx.mock
    def test_add_roles_to_group(self, root_client: CompassRootClient, respx_mock: MockRouter) -> None:
        mock_response = [
            {
                "role_name": "admin",
                "group_name": "developers",
            },
            {
                "role_name": "editor",
                "group_name": "developers",
            },
        ]

        respx_mock.post("http://test.com/security/admin/rbac/v2/groups/developers/roles").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = root_client.add_roles_to_group("developers", ["admin", "editor"])

        assert len(result) == 2
        assert all(isinstance(r, GroupRole) for r in result)
        assert result[0].role_name == "admin"

    @respx.mock
    def test_remove_roles_from_group(self, root_client: CompassRootClient, respx_mock: MockRouter) -> None:
        mock_response = [
            {
                "role_name": "admin",
                "group_name": "developers",
            },
            {
                "role_name": "editor",
                "group_name": "developers",
            },
        ]

        respx_mock.delete("http://test.com/security/admin/rbac/v2/groups/developers/roles/admin,editor").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        result = root_client.remove_roles_from_group("developers", ["admin", "editor"])

        assert len(result) == 2
        assert all(isinstance(r, GroupRole) for r in result)

    @respx.mock
    def test_get_group_roles_page(
        self,
        root_client: CompassRootClient,
        mock_roles_page: dict[str, Any],
        respx_mock: MockRouter,
    ) -> None:
        respx_mock.get("http://test.com/security/admin/rbac/v2/groups/developers/roles").mock(
            return_value=httpx.Response(200, json=mock_roles_page)
        )

        result = root_client.get_group_roles_page("developers")

        assert isinstance(result, RolesPage)
        assert len(result.roles) == 2

    @respx.mock
    def test_get_role_groups_page(
        self,
        root_client: CompassRootClient,
        mock_groups_page: dict[str, Any],
        respx_mock: MockRouter,
    ) -> None:
        respx_mock.get("http://test.com/security/admin/rbac/v2/roles/admin/groups").mock(
            return_value=httpx.Response(200, json=mock_groups_page)
        )

        result = root_client.get_role_groups_page("admin")

        assert isinstance(result, GroupsPage)
        assert len(result.groups) == 2

    @respx.mock
    def test_error_handling(self, root_client: CompassRootClient, respx_mock: MockRouter) -> None:
        respx_mock.get("http://test.com/security/admin/rbac/v2/users").mock(
            return_value=httpx.Response(404, json={"error": "Not found"})
        )

        with pytest.raises(httpx.HTTPStatusError):
            root_client.get_users_page()

    @respx.mock
    def test_previous_page_direction(
        self,
        root_client: CompassRootClient,
        mock_users_page: dict[str, Any],
        respx_mock: MockRouter,
    ) -> None:
        page_info = PageInfo(
            next=None,
            previous="prev_cursor",
            filter=None,
            total=10,
            page=1,
            total_pages=1,
            page_size=10,
        )
        respx_mock.get(
            "http://test.com/security/admin/rbac/v2/users",
            params={"cursor": "prev_cursor"},
        ).mock(return_value=httpx.Response(200, json=mock_users_page))

        result = root_client.get_users_page(page_info=page_info, direction=PageDirection.PREVIOUS)

        assert isinstance(result, UsersPage)

    def test_headers_initialization(self) -> None:
        client = CompassRootClient(
            compass_url="http://test.com",
            root_user_token="my_token",
        )
        assert client.headers["Authorization"] == "Bearer my_token"
        assert client.headers["Content-Type"] == "application/json"
        assert client.base_url == "http://test.com/security/admin/rbac"
