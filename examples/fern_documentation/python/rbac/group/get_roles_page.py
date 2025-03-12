from typing import Optional

from cohere.compass.clients.access_control import CompassRootClient
from cohere.compass.models.access_control import PageDirection, PageInfo, Role, User

COMPASS_API_URL = ...
ROOT_USER_TOKEN = ...
GROUP_NAME = ...

compass_root_client = CompassRootClient(
    compass_url=COMPASS_API_URL,
    root_user_token=ROOT_USER_TOKEN,
)


def get_all_roles_in_group(
    group_name: str,
    filter: Optional[str] = None,
    page_info: Optional[PageInfo] = None,
    direction: Optional[PageDirection] = None,
    roles: Optional[list[Role]] = None,
) -> list[User]:
    roles_page = compass_root_client.get_group_roles_page(
        group_name=group_name,
        filter=filter,
        page_info=page_info,
        direction=direction,
    )

    roles = roles or []
    roles.extend(roles_page.roles)

    if not roles_page.page_info.has_next_page():
        return roles

    return get_all_roles_in_group(
        group_name=group_name,
        filter=filter,
        page_info=roles_page.page_info,
        direction=PageDirection.NEXT,
        roles=roles,
    )


roles = get_all_roles_in_group(group_name=GROUP_NAME)
print(f"Roles in group: {roles}")
