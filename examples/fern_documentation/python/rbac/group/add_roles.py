from cohere.compass.clients.access_control import CompassRootClient
from cohere.compass.models.access_control import Group, Permission, Policy, Role
from requests.exceptions import HTTPError

COMPASS_API_URL = ...
ROOT_USER_TOKEN = ...

compass_root_client = CompassRootClient(
    compass_url=COMPASS_API_URL,
    root_user_token=ROOT_USER_TOKEN,
)

try:
    groups = compass_root_client.create_groups(
        groups=[
            Group(group_name="unique_group_name_1"),
            Group(group_name="unique_group_name_2"),
        ]
    )
except HTTPError as e:
    if e.response.status_code == 409:
        print("Group already exists.")
    else:
        raise

try:
    roles = compass_root_client.create_roles(
        roles=[
            Role(
                role_name="developer_read_specific_index",
                policies=[Policy(indexes=["user-*"], permission=Permission.READ)],
            ),
        ]
    )
except HTTPError as e:
    if e.response.status_code == 409:
        print("Role already exists.")
    else:
        raise

group_roles = compass_root_client.add_roles_to_group(
    group_name="unique_group_name_1",
    role_names=[
        "developer_read_specific_index",
    ],
)

for group_role in group_roles:
    print(f"Added roles: {group_role.role_name} to group: {group_role.group_name}")
