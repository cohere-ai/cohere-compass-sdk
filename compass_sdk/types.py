from enum import Enum
from typing import List

from pydantic import BaseModel


class UserFetchResponse(BaseModel):
    name: str


class UserCreateRequest(BaseModel):
    name: str


class UserCreateResponse(BaseModel):
    name: str
    token: str


class UserDeleteResponse(BaseModel):
    name: str


class GroupFetchResponse(BaseModel):
    name: str
    user_name: str


class GroupCreateRequest(BaseModel):
    name: str
    user_names: List[str]


class GroupCreateResponse(BaseModel):
    name: str
    user_name: str


class GroupUserDeleteResponse(BaseModel):
    group_name: str
    user_name: str


class Permission(Enum):
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"
    ROOT = "root"


class PolicyRequest(BaseModel):
    indexes: List[str]
    permission: Permission


class PolicyResponse(BaseModel):
    indexes: List[str]
    permission: str


class RoleFetchResponse(BaseModel):
    name: str
    policies: List[PolicyResponse]


class RoleCreateRequest(BaseModel):
    name: str
    policies: List[PolicyRequest]


class RoleCreateResponse(BaseModel):
    name: str
    policies: List[PolicyResponse]


class RoleDeleteResponse(BaseModel):
    name: str


class RoleMappingRequest(BaseModel):
    role_name: str
    group_name: str


class RoleMappingResponse(BaseModel):
    role_name: str
    group_name: str


class RoleMappingDeleteResponse(BaseModel):
    role_name: str
    group_name: str
