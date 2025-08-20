# Cohere Compass SDK

[![Checked with pyright](https://microsoft.github.io/pyright/img/pyright_badge.svg)](https://microsoft.github.io/pyright/)

The Compass SDK is a Python library that allows you to parse documents and insert them
into a Compass index.

In order to parse documents, the Compass SDK relies on the Compass Parser API, which is
a RESTful API that receives files and returns parsed documents. This requires a hosted
Compass server.

The Compass SDK provides a `CompassParserClient` that allows to interact with the parser
API from your Python code in a convenient manner. The `CompassParserClient` provides
methods to parse single and multiple files, as well as entire folders, and supports
multiple file types (e.g., `pdf`, `docx`, `json`, `csv`, etc.) as well as different file
systems (e.g., local, S3, GCS, etc.).

To insert parsed documents into a `Compass` index, the Compass SDK provides a
`CompassClient` class that allows to interact with a Compass API server. The Compass API
is also a RESTful API that allows to create, delete and search documents in a Compass
index. To install a Compass API service, please refer to the [Compass
documentation](https://github.com/cohere-ai/compass)

## Table of Contents

<!--
Do NOT remove the line below; it is used by markdown-toc to automatically generate the
Table of Contents.

To update the Table Of Contents, execute the following command in the repo root dir:

markdown-toc -i README.md

If you don't have the markdown-toc tool, you can install it with:

npm i -g markdown-toc # use sudo if you use a system-wide node installation.
>

<!-- toc -->

- [Getting Started](#getting-started)
- [Local Development](#local-development)
  - [Create Python Virtual Environment](#create-python-virtual-environment)
  - [Running Tests Locally](#running-tests-locally)
    - [VSCode Users](#vscode-users)
  - [Pre-commit](#pre-commit)

<!-- tocstop -->

## Getting Started

Fill in your URL, username, password, and path to test data below for an end to end run
of parsing and searching.

### Installation

```bash
pip install cohere-compass-sdk
```

```Python
from cohere_compass.clients.compass import CompassClient
from cohere_compass.clients.parser import CompassParserClient
from cohere_compass.models.config import MetadataStrategy, MetadataConfig

api_url = "<COMPASS_URL>"
parser_url = "<PARSER URL>"
bearer_token = "<PASS BEARER TOKEN IF ANY OTHERWISE LEAVE IT BLANK>"

index = "test-index"
data_to_index = "<PATH_TO_TEST_DATA>"

# Parse the files before indexing
parsing_client = CompassParserClient(parser_url = parser_url)
metadata_config = MetadataConfig(
    metadata_strategy=MetadataStrategy.No_Metadata,
    commandr_extractable_attributes=["date", "link", "page_title", "authors"]
)

docs_to_index = parsing_client.process_folder(folder_path=data_to_index, metadata_config=metadata_config, recursive=True)

# Create index and insert files
compass_client = CompassClient(index_url=api_url, bearer_token=bearer_token)
compass_client.create_index(index_name=index)
results = compass_client.insert_docs(index_name=index, docs=docs_to_index)

result = compass_client.search_chunks(index_name=index, query="test", top_k=1)
print(f"Results preview: \n {result.hits} ... \n \n ")
```

### Adding filters to documents

#### Adding filter via dict

```python
from cohere_compass.clients.compass import CompassClient
from cohere_compass.clients.parser import CompassParserClient
from cohere_compass.models.search import SearchFilter

api_url = "<COMPASS_URL>"
parser_url = "<PARSER URL>"
data_to_index = "<PATH_TO_TEST_DATA>"
index = "test-index"
bearer_token = "<PASS BEARER TOKEN IF ANY OTHERWISE LEAVE IT BLANK>"

parsing_client = CompassParserClient(parser_url = parser_url)
custom_context_dict = {
    "doc_purpose": "demo"
}

docs_to_index = parsing_client.process_folder(folder_path=data_to_index, recursive=True, custom_context=custom_context_dict)

compass_client = CompassClient(index_url=api_url, bearer_token=bearer_token)
filter = SearchFilter(type=SearchFilter.FilterType.EQ, field="content.doc_purpose", value="demo")
result = compass_client.search_chunks(index_name=index, query="*", filters=[filter])
print(f"Results preview: \n {result.hits} ... \n \n ")
```

#### Adding filter via function

```python
from cohere_compass.clients.compass import CompassClient
from cohere_compass.clients.parser import CompassParserClient
from cohere_compass.models.search import SearchFilter
from cohere_compass.models.documents import CompassDocument

api_url = "<COMPASS_URL>"
parser_url = "<PARSER URL>"
data_to_index = "<PATH_TO_TEST_DATA>"
index = "test-index"
bearer_token = "<PASS BEARER TOKEN IF ANY OTHERWISE LEAVE IT BLANK>"

parsing_client = CompassParserClient(parser_url = parser_url)

def custom_context_fn(input: CompassDocument):
    content = input.content
    if len(input.chunks) > 2:
        content["new_doc_field"] = "more_than_two_chunks"
    else:
        content["new_doc_field"] = "less_than_two_chunks"
    return content


docs_to_index = parsing_client.process_folder(folder_path=data_to_index, recursive=True, custom_context=custom_context_fn)

compass_client = CompassClient(index_url=api_url, bearer_token=bearer_token)
filter = SearchFilter(type=SearchFilter.FilterType.EQ, field="content.new_doc_field", value="less_than_two_chunks")
result = compass_client.search_chunks(index_name=index, query="*", filters=[filter])
print(f"Results preview: \n {result.hits} ... \n \n ")
```

### RBAC

```python
from cohere_compass.clients.access_control import CompassRootClient
from cohere_compass.models.access_control import Group, Permission, Policy, Role, User
from requests.exceptions import HTTPError

ROOT_BEARER_TOKEN = "<ROOT_BEARER_TOKEN>"
API_URL = "<API_URL>"
compass_root = CompassRootClient(API_URL, ROOT_BEARER_TOKEN)

user = User(user_name="<USER_NAME>")
group = Group(group_name="<GROUP_NAME>")
role = Role(role_name="<ROLE_NAME>")
indexes = ["<ALLOWED_INDEX or REGEX>"]
permission = Permission.WRITE # or Permission.READ

try:
    # Create Users
    users = client.create_users([user])

    # Create Groups
    groups = client.create_groups([group])

    # Add Users to a Group
    memberships = client.add_members_to_group(group.group_name, [user.user_name])

    # Add Policies and Create a Role
    role.policies = [
        Policy(permission=Permission.READ, indexes=indexes),
    ]
    roles = client.create_roles([role])

    # Update Role Policies
    role.policies = [
        Policy(permission=Permission.READ, indexes=indexes),
        Policy(permission=Permission.WRITE, indexes=indexes),
    ]
    role = client.update_role(role)

    # Assign Roles to a Group
    role_assignments = client.add_roles_to_group(group.group_name, [role.role_name])

    # Token for the user to access the indexes
    USER_TO_TOKENS = {user.name: user.token for user in users}
except HTTPError as e:
    if e.response.status_code == 409:
        print("A entity already exists", e.response.json())
```

### Reading RBAC Information

```python
from cohere_compass.clients.access_control import CompassRootClient
from cohere_compass.models.access_control import Group, Role, User, PageDirection
from requests.exceptions import HTTPError

ROOT_BEARER_TOKEN = "<ROOT_BEARER_TOKEN>"
API_URL = "<API_URL>"
compass_root = CompassRootClient(API_URL, ROOT_BEARER_TOKEN)

user = User(user_name="<USER_NAME>")
group = Group(group_name="<GROUP_NAME>")
role = Role(role_name="<ROLE_NAME>")

# List all Users in the RBAC system
# First page
user_page = client.get_users_page()
# Subsequent pages
user_page = client.get_users_page(page_info=user_page.page_info, direction=PageDirection.NEXT)

# List all Groups in the RBAC system
# First page
group_page = client.get_groups_page()
# Subsequent pages
group_page = client.get_groups_page(page_info=group_page.page_info, direction=PageDirection.NEXT)

# List all Roles in the RBAC system
# First page
role_page = client.get_roles_page()
# Subsequent pages
role_page = client.get_roles_page(page_info=role_page.page_info, direction=PageDirection.NEXT)

# Get the Group Details (all data + first page each of Users who are Members and Roles Assigned)
detailed_group = client.get_detailed_group(group.group_name)

# Get pages of Group's User Memberships
# First page
memberships = client.get_group_members_page(group.group_name)
# Subsequent pages (can use the users_page_info from details)
memberships = client.get_group_members_page(group.group_name, page_info=memberships.page_info, direction=PageDirection.NEXT)

# Get pages of Group's Roles Assignments
# First page
role_assignments = client.get_group_roles_page(group.group_name)
# Subsequent pages (can use the role_page_info from details)
role_assignments = client.get_group_roles_page(group.group_name, page_info=role_assignments.page_info, direction=PageDirection.NEXT)

# Get the User Details (all data + first page of Groups that the User is a Member of)
detailed_user = client.get_detailed_user(user.user_name)

# Get pages of User's Group Memberships
# First page
group_memberships = client.get_user_groups_page(user.user_name)
# Subsequent pages (can use the group_page_info from details)
group_memberships = client.get_user_groups_page(user.user_name, page_info=group_memberships.page_info, direction=PageDirection.NEXT)

# Get the Roles Details (all data + first page of Groups the Role is Assigned to)
detailed_role = client.get_detailed_role(role.role_name)

# Get pages of Role's Group Assignments
group_assignments = client.get_role_groups_page(role.role_name)
# Subsequent pages (can use the group_page_info from details)
group_assignments = client.get_role_groups_page(role.role_name, page_info=group_assignments.page_info, direction=PageDirection.NEXT)

# Filtering any Page type query, exemplified on Users Page, but works with all.
user_page = client.get_users_page(filter="<SOME_NAME_OR_NAME_PARTIAL>")
```

### Deleting RBAC

```python
from cohere_compass.clients.access_control import CompassRootClient
from cohere_compass.models.access_control import Group, Role, User

ROOT_BEARER_TOKEN = "<ROOT_BEARER_TOKEN>"
API_URL = "<API_URL>"
compass_root = CompassRootClient(API_URL, ROOT_BEARER_TOKEN)

user = User(user_name="<USER_NAME>")
group = Group(group_name="<GROUP_NAME>")
role = Role(role_name="<ROLE_NAME>")

# removing Roles from a Group
removed_roles = client.remove_roles_from_group(group.group_name, [role.role_name])

# removing Members from a Group
removed_members = client.remove_members_from_group(group.group_name, [user.user_name])

# deleting Roles
deleted_roles = client.delete_roles([role.role_name])

# deleting Groups
deleted_groups = client.delete_groups([group.group_name])

# deleting Users
deleted_users = client.delete_users([user.user_name])
```

## Local Development

### Create Python Virtual Environment

We use Poetry to manage our Python environment. To create the virtual environment use
the following command:

```
poetry install
```

### Running Tests Locally

We use `pytest` for testing. So, you can simply run tests using the following command:

```
poetry run python -m pytest
```

#### VSCode Users

We provide `.vscode` folder for those developers who prefer to use VSCode. You just need
to open the folder in VSCode and VSCode should pick our settings.

### Pre-commit

We love and appreciate Coding Standards and so we enforce them in our code base.
However, without automation, enforcing Coding Standards usually result in a lot of
frustration for developers when they publish Pull Requests and our linters complain. So,
we automate our formatting and linting with [pre-commit](https://pre-commit.com/). All
you need to do is install our `pre-commit` hook so the code gets formatted automatically
when you commit your changes locally:

```bash
pip install pre-commit
```
