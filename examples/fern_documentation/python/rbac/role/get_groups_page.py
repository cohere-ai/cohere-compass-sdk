from typing import Optional

from cohere.compass.clients.access_control import CompassRootClient
from cohere.compass.models.access_control import Group, PageDirection, PageInfo

COMPASS_API_URL = ...
ROOT_USER_TOKEN = ...
ROLE_NAME = ...

compass_root_client = CompassRootClient(
    compass_url=COMPASS_API_URL,
    root_user_token=ROOT_USER_TOKEN,
)


def get_all_role_groups(
    role_name: str,
    filter: Optional[str] = None,
    page_info: Optional[PageInfo] = None,
    groups: Optional[list[Group]] = None,
    direction: Optional[PageDirection] = None,
) -> list[Group]:
    role_pages = compass_root_client.get_role_groups_page(
        role_name=role_name, filter=filter, page_info=page_info, direction=direction
    )

    groups = groups or []
    groups.extend(role_pages.groups)

    if not role_pages.page_info.has_next_page():
        return groups

    return get_all_role_groups(
        role_name=role_name,
        filter=filter,
        page_info=role_pages.page_info,
        groups=groups,
        direction=PageDirection.NEXT,
    )


group = get_all_role_groups(role_name=ROLE_NAME)
print(f"Groups in role: {group}")
