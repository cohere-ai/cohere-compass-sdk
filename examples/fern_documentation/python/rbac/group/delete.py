from cohere_compass.clients.access_control import CompassRootClient

COMPASS_API_URL = ...
ROOT_USER_TOKEN = ...
GROUP_NAME = ...

compass_root_client = CompassRootClient(
    compass_url=COMPASS_API_URL,
    root_user_token=ROOT_USER_TOKEN,
)
groups = compass_root_client.delete_groups(group_names=[GROUP_NAME])

for group in groups:
    print(f"Deleted group: {group.group_name}")
