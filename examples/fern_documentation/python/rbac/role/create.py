from cohere_compass.clients.access_control import CompassRootClient
from cohere_compass.models.access_control import Permission, Policy, Role

COMPASS_API_URL = ...
ROOT_USER_TOKEN = ...

compass_root_client = CompassRootClient(
    compass_url=COMPASS_API_URL,
    root_user_token=ROOT_USER_TOKEN,
)

roles = compass_root_client.create_roles(
    roles=[
        # * allows all index
        Role(
            role_name="developer_read_all_index",
            policies=[Policy(indexes=["*"], permission=Permission.READ)],
        ),
        Role(
            role_name="developer_write_all_index",
            policies=[Policy(indexes=["*"], permission=Permission.WRITE)],
        ),
        # user-* allows index only prefixed with `user-`
        Role(
            role_name="developer_read_specific_index",
            policies=[Policy(indexes=["user-*"], permission=Permission.READ)],
        ),
    ]
)

for role in roles:
    print(f"Role information: {role.role_name}, {role.policies}")
