from cohere.compass.clients.access_control import CompassRootClient

COMPASS_API_URL = ...
ROOT_USER_TOKEN = ...
ROLE_NAME = ...

compass_root_client = CompassRootClient(
    compass_url=COMPASS_API_URL,
    root_user_token=ROOT_USER_TOKEN,
)
detailed_role = compass_root_client.get_detailed_role(role_name=ROLE_NAME)

print(f"Role name: {detailed_role.role_name}")
print(f"Group name: {detailed_role.groups}")
