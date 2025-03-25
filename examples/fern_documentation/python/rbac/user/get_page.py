from typing import Optional

from cohere_compass.clients.access_control import CompassRootClient
from cohere_compass.models.access_control import PageDirection, PageInfo, User

COMPASS_API_URL = ...
ROOT_USER_TOKEN = ...

compass_root_client = CompassRootClient(
    compass_url=COMPASS_API_URL,
    root_user_token=ROOT_USER_TOKEN,
)


def get_all_users(
    filter: Optional[str] = None,
    page_info: Optional[PageInfo] = None,
    direction: Optional[PageDirection] = None,
    users: Optional[list[User]] = None,
) -> list[User]:
    user_page = compass_root_client.get_users_page(
        filter=filter,
        page_info=page_info,
        direction=direction,
    )

    users = users if users else []
    users.extend(user_page.users)
    if not user_page.page_info.has_next_page():
        return users
    return get_all_users(
        filter=filter,
        page_info=user_page.page_info,
        direction=PageDirection.NEXT,
        users=users,
    )


users = get_all_users()

for user in users:
    print(f"User name {user.user_name}")
