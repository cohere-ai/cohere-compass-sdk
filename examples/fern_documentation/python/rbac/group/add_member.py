from cohere.compass.clients.access_control import CompassRootClient
from cohere.compass.models.access_control import Group, User
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

    for group in groups:
        print(f"Created group: {group.group_name}")
except HTTPError as e:
    if e.response.status_code == 409:
        print("Group already exists.")
    else:
        raise

try:
    users = compass_root_client.create_users(
        users=[
            User(user_name="unique_user_name_1"),
            User(user_name="unique_user_name_2"),
        ]
    )
except HTTPError as e:
    if e.response.status_code == 409:
        print("User already exists.")
    else:
        raise

try:
    # Adding unique_user_name_1 to unique_group_name_1
    group_membership = compass_root_client.add_members_to_group(
        group_name="unique_group_name_1",
        user_names=["unique_user_name_1"],
    )
    print("Group membership added: ", group_membership)
except HTTPError as e:
    if e.response.status_code == 409:
        print("Membership already exists.")
    else:
        raise

try:
    # Adding unique_user_name_2 to unique_group_name_2
    group_membership = compass_root_client.add_members_to_group(
        group_name="unique_group_name_2",
        user_names=["unique_user_name_2"],
    )
    print("Group membership added: ", group_membership)
except HTTPError as e:
    if e.response.status_code == 409:
        print("Membership already exists.")
    else:
        raise
