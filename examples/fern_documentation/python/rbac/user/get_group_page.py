from typing import Optional

from cohere.compass.clients.access_control import CompassRootClient
from cohere.compass.models.access_control import Group, PageDirection, PageInfo

COMPASS_API_URL = ...
ROOT_USER_TOKEN = ...
USER_NAME = ...

compass_root_client = CompassRootClient(
    compass_url=COMPASS_API_URL,
    root_user_token=ROOT_USER_TOKEN,
)


def get_all_user_groups(
    user_name: str,
    filter: Optional[str] = None,
    page_info: Optional[PageInfo] = None,
    groups: Optional[list[Group]] = None,
    direction: Optional[PageDirection] = None,
) -> list[Group]:
    group_pages = compass_root_client.get_user_groups_page(
        user_name=user_name, filter=filter, page_info=page_info, direction=direction
    )

    groups = groups or []
    groups.extend(group_pages.groups)

    if not group_pages.page_info.has_next_page():
        return groups

    return get_all_user_groups(
        user_name=user_name,
        filter=filter,
        page_info=group_pages.page_info,
        groups=groups,
        direction=PageDirection.NEXT,
    )


groups = get_all_user_groups(user_name=USER_NAME)
print(f"User groups: {groups}")
