from cohere_compass.clients.access_control import CompassRootClient

COMPASS_API_URL = ...
ROOT_USER_TOKEN = ...
ROLE_NAME = ...

compass_root_client = CompassRootClient(
    compass_url=COMPASS_API_URL,
    root_user_token=ROOT_USER_TOKEN,
)

roles = compass_root_client.delete_roles(role_names=[ROLE_NAME])

for role in roles:
    print(f"Deleted role: {role.role_name}")
