from typing import Optional

from cohere.compass.clients.access_control import CompassRootClient
from cohere.compass.models.access_control import PageDirection, PageInfo, User

COMPASS_API_URL = ...
ROOT_USER_TOKEN = ...
GROUP_NAME = ...
compass_root_client = CompassRootClient(
    compass_url=COMPASS_API_URL,
    root_user_token=ROOT_USER_TOKEN,
)


def get_all_members_in_group(
    group_name: str,
    filter: Optional[str] = None,
    page_info: Optional[PageInfo] = None,
    direction: Optional[PageDirection] = None,
    users: Optional[list[User]] = None,
) -> list[User]:
    user_page = compass_root_client.get_group_members_page(
        group_name=group_name,
        filter=filter,
        page_info=page_info,
        direction=direction,
    )

    users = users or []
    users.extend(user_page.users)

    if not user_page.page_info.has_next_page():
        return users
    return get_all_members_in_group(
        group_name=group_name,
        filter=filter,
        page_info=user_page.page_info,
        direction=PageDirection.NEXT,
        users=users,
    )


members = get_all_members_in_group(group_name=GROUP_NAME)
print(f"Members in group: {members}")
