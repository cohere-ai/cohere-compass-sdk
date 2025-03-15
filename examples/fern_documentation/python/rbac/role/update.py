from cohere.compass.clients.access_control import CompassRootClient
from cohere.compass.models.access_control import Permission, Policy, Role

COMPASS_API_URL = ...
ROOT_USER_TOKEN = ...

compass_root_client = CompassRootClient(
    compass_url=COMPASS_API_URL,
    root_user_token=ROOT_USER_TOKEN,
)

role = compass_root_client.update_role(
    role=Role(
        role_name="developer_read_all_index",
        policies=[Policy(indexes=["prefix-*"], permission=Permission.READ)],
    )
)

print(f"Updated role information: {role.role_name}, {role.policies}")
