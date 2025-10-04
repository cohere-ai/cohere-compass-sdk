from cohere_compass.clients.access_control import CompassRootClient
from cohere_compass.models.access_control import DetailedUser

COMPASS_API_URL = "<COMPASS_API_URL>"
ROOT_USER_TOKEN = "<ROOT_USER_TOKEN>"
USER_NAME = "<USER_NAME>"

compass_root_client = CompassRootClient(
    compass_url=COMPASS_API_URL,
    root_user_token=ROOT_USER_TOKEN,
)

detailed_user: DetailedUser = compass_root_client.get_detailed_user(user_name=USER_NAME)

print(f"User name {detailed_user.user_name}")
print(f"User groups {detailed_user.groups}")
