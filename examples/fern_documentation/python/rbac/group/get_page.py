from typing import Optional

from cohere_compass.clients.access_control import CompassRootClient
from cohere_compass.models.access_control import Group, PageDirection, PageInfo

COMPASS_API_URL = ...
ROOT_USER_TOKEN = ...

compass_root_client = CompassRootClient(
    compass_url=COMPASS_API_URL,
    root_user_token=ROOT_USER_TOKEN,
)


def get_all_groups(
    filter: Optional[str] = None,
    page_info: Optional[PageInfo] = None,
    direction: Optional[PageDirection] = None,
    groups: Optional[list[Group]] = None,
) -> list[Group]:
    group_pages = compass_root_client.get_groups_page(
        filter=filter, page_info=page_info, direction=direction
    )

    groups = groups or []
    groups.extend(group_pages.groups)

    if not group_pages.page_info.has_next_page():
        return groups
    return get_all_groups(
        filter=filter,
        page_info=group_pages.page_info,
        direction=PageDirection.NEXT,
        groups=groups,
    )


groups = get_all_groups()
print(f"Groups: {groups}")
