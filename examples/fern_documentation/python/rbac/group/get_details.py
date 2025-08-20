from cohere_compass.clients.access_control import CompassRootClient

COMPASS_API_URL = ...
ROOT_USER_TOKEN = ...
GROUP_NAME = ...

compass_root_client = CompassRootClient(
    compass_url=COMPASS_API_URL,
    root_user_token=ROOT_USER_TOKEN,
)

group = compass_root_client.get_detailed_group(group_name=GROUP_NAME)

print(f"Group roles: {group.roles}")
print(f"Group users: {group.users}")
