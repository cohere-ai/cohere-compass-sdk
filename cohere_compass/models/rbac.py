# Python imports
from enum import Enum

# 3rd party imports
from pydantic import BaseModel


class UserFetchResponse(BaseModel):
    """Response model for fetching user details."""

    name: str


class UserCreateRequest(BaseModel):
    """Request model for creating a new user."""

    name: str


class UserCreateResponse(BaseModel):
    """Response model for creating a new user."""

    name: str
    token: str


class UserDeleteResponse(BaseModel):
    """Response model for deleting a user."""

    name: str


class GroupFetchResponse(BaseModel):
    """Response model for fetching group details."""

    name: str
    user_name: str


class GroupCreateRequest(BaseModel):
    """Request model for creating a new group."""

    name: str
    user_names: list[str]


class GroupCreateResponse(BaseModel):
    """Response model for creating a new group."""

    name: str
    user_name: str


class GroupDeleteResponse(BaseModel):
    """Response model for deleting a group."""

    name: str


class GroupUserDeleteResponse(BaseModel):
    """Response model for removing a user from a group."""

    group_name: str
    user_name: str


class Permission(Enum):
    """Enumeration for user permissions."""

    READ = "read"
    WRITE = "write"


class PolicyRequest(BaseModel):
    """Request model for creating a policy."""

    indexes: list[str]
    permission: Permission


class PolicyResponse(BaseModel):
    """Response model for retrieving a policy."""

    indexes: list[str]
    permission: str


class RoleFetchResponse(BaseModel):
    """Response model for fetching role details."""

    name: str
    policies: list[PolicyResponse]


class RoleCreateRequest(BaseModel):
    """Request model for creating a new role."""

    name: str
    policies: list[PolicyRequest]


class RoleCreateResponse(BaseModel):
    """Response model for creating a new role."""

    name: str
    policies: list[PolicyResponse]


class RoleDeleteResponse(BaseModel):
    """Response model for deleting a role."""

    name: str


class RoleMappingRequest(BaseModel):
    """Request model for mapping a role to a group."""

    role_name: str
    group_name: str


class RoleMappingResponse(BaseModel):
    """Response model for retrieving role-to-group mapping details."""

    role_name: str
    group_name: str


class RoleMappingDeleteResponse(BaseModel):
    """Response model for deleting a role-to-group mapping."""

    role_name: str
    group_name: str
