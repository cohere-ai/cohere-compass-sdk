# Python imports
from enum import Enum
from typing import Optional

# 3rd party imports
from pydantic import BaseModel


class PageDirection(Enum):
    """Enumeration for pagination direction."""

    NEXT = "next"
    PREVIOUS = "previous"


class PageInfo(BaseModel):
    """Model for pagination information."""

    total: int
    page: int
    filter: Optional[str] = None
    total_pages: int
    next: Optional[str] = None
    previous: Optional[str] = None
    page_size: int

    def has_next_page(self) -> bool:
        """Check if there is a next page."""
        return self.next is not None

    def has_previous_page(self) -> bool:
        """Check if there is a previous page."""
        return self.previous is not None


class User(BaseModel):
    """Model for user details."""

    user_name: str


class UserWithToken(User):
    """Model for user details with token."""

    token: str


class UsersPage(BaseModel):
    """Response model for fetching lists of Users."""

    users: list[User]
    page_info: PageInfo


class Permission(Enum):
    """Enumeration for permissions."""

    READ = "read"
    WRITE = "write"
    ROOT = "root"


class Policy(BaseModel):
    """Model for policy details."""

    indexes: list[str]
    permission: Permission


class Role(BaseModel):
    """Model for role details."""

    role_name: str
    policies: list[Policy] = []


class RolesPage(BaseModel):
    """Response model for fetching lists of Roles."""

    roles: list[Role]
    page_info: PageInfo


class Group(BaseModel):
    """Model for group details."""

    group_name: str


class GroupsPage(BaseModel):
    """Response model for fetching lists of Groups."""

    groups: list[Group]
    page_info: PageInfo


class DetailedUser(User):
    """Model for detailed User information."""

    groups: list[Group]
    groups_page_info: PageInfo


class DetailedRole(Role):
    """Model for detailed Role information."""

    groups: list[Group]
    groups_page_info: PageInfo


class DetailedGroup(Group):
    """Model for detailed Group information."""

    roles: list[Role]
    roles_page_info: PageInfo
    users: list[User]
    users_page_info: PageInfo


class GroupMembership(BaseModel):
    """Model for Group membership details."""

    group_name: str
    user_name: str


class GroupRole(BaseModel):
    """Model for Group roles details."""

    group_name: str
    role_name: str
