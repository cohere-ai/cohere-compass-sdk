from typing import Optional

from cohere_compass.clients.access_control import CompassRootClient
from cohere_compass.models.access_control import PageDirection, PageInfo, Role

COMPASS_API_URL = "<COMPASS_API_URL>"
ROOT_USER_TOKEN = "<ROOT_USER_TOKEN>"

compass_root_client = CompassRootClient(
    compass_url=COMPASS_API_URL,
    root_user_token=ROOT_USER_TOKEN,
)


def get_all_roles(
    filter: Optional[str] = None,
    page_info: Optional[PageInfo] = None,
    roles: Optional[list[Role]] = None,
    direction: Optional[PageDirection] = None,
) -> list[Role]:
    roles_page = compass_root_client.get_roles_page(
        filter=filter, page_info=page_info, direction=direction
    )

    roles = roles or []
    roles.extend(roles_page.roles)

    if not roles_page.page_info.has_next_page():
        return roles

    return get_all_roles(
        filter=filter, page_info=page_info, direction=PageDirection.NEXT
    )


roles = get_all_roles()
print(f"Roles: {roles}")
