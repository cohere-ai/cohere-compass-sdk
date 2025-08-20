from cohere_compass.clients.access_control import CompassRootClient
from cohere_compass.models.access_control import Group

COMPASS_API_URL = ...
ROOT_USER_TOKEN = ...

compass_root_client = CompassRootClient(
    compass_url=COMPASS_API_URL,
    root_user_token=ROOT_USER_TOKEN,
)

groups = compass_root_client.create_groups(
    groups=[
        Group(group_name="unique_group_name_1"),
        Group(group_name="unique_group_name_2"),
    ]
)

for group in groups:
    print(f"Created group: {group.group_name}")
