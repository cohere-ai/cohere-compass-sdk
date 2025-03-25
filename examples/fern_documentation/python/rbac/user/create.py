from cohere_compass.clients.access_control import CompassRootClient
from cohere_compass.models.access_control import User

COMPASS_API_URL = ...
ROOT_USER_TOKEN = ...

compass_root_client = CompassRootClient(
    compass_url=COMPASS_API_URL,
    root_user_token=ROOT_USER_TOKEN,
)

users_with_token = compass_root_client.create_users(
    users=[User(user_name="unique_user_name_1"), User(user_name="unique_user_name_2")]
)

for user in users_with_token:
    # Token is used to access the Compass API on behalf of the user
    print(f"User information: {user.user_name}, Token: {user.token}")
