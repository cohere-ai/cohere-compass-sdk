from cohere.compass.clients.access_control import CompassRootClient

COMPASS_API_URL = ...
ROOT_USER_TOKEN = ...

compass_root_client = CompassRootClient(
    compass_url=COMPASS_API_URL,
    root_user_token=ROOT_USER_TOKEN,
)

users = compass_root_client.delete_users(
    user_names=["unique_user_name_1", "unique_user_name_2"]
)

for user in users:
    print(f"Deleted users: {user.user_name}")
