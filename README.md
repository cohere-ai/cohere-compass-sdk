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

```Python
from cohere.compass.clients.compass import CompassClient
from cohere.compass.clients.parser import CompassParserClient
from cohere.compass.models.config import MetadataStrategy, MetadataConfig

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
from cohere.compass.clients.compass import CompassClient
from cohere.compass.clients.parser import CompassParserClient
from cohere.compass.models.search import SearchFilter

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
from cohere.compass.clients.compass import CompassClient
from cohere.compass.clients.parser import CompassParserClient
from cohere.compass.models.search import SearchFilter
from cohere.compass.models.documents import CompassDocument

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
from cohere.compass.clients.rbac import CompassRootClient
from cohere.compass.clients.rbac import UserCreateRequest
from cohere.compass.models.rbac import GroupCreateRequest, RoleCreateRequest, RoleMappingRequest, Permission, PolicyRequest
from cohere.compass.clients.rbac import UserCreateResponse
from requests.exceptions import HTTPError

ROOT_BEARER_TOKEN = "<ROOT_BEARER_TOKEN>"
API_URL = "<API_URL>"
compass_root = CompassRootClient(API_URL, ROOT_BEARER_TOKEN)

user_names = ["<USER_NAME>"]
group_name = "<GROUP_NAME>"
role_name = "<ROLE_NAME>"
indexes = ["<ALLOWED_INDEX or REGEX>"]
permission = Permission.WRITE # or Permission.READ
 
try:
    # Create users
    users_create_request = [UserCreateRequest(name=user_name) for user_name in user_names]
    users: list[UserCreateResponse] = compass_root.create_users(users=users_create_request)
    
    # Add users to group
    compass_root.create_groups(groups=[GroupCreateRequest(name=group_name, user_names=user_names)])
    
    # Create a role
    compass_root.create_roles(roles=[RoleCreateRequest(name=role_name, policies=[PolicyRequest(indexes=indexes, permission=permission)])])
    # Map role to groups
    compass_root.create_role_mappings(role_mappings=[RoleMappingRequest(role_name=role_name, group_name=group_name)])
    # Token for the user to access the indexes
    USER_TO_TOKENS = {user.name: user.token for user in users}
except HTTPError as e:
    if e.response.status_code == 409:
        print("A entity already exists", e.response.json())
```

### Deleting RBAC
```python
from cohere.compass.clients.rbac import CompassRootClient

ROOT_BEARER_TOKEN = "<ROOT_BEARER_TOKEN>"
API_URL = "<API_URL>"
compass_root = CompassRootClient(API_URL, ROOT_BEARER_TOKEN)

user_names = ["<USER_NAME>"]
group_names = ["<GROUP_NAME>"]
role_names = ["<ROLE_NAME>"]

compass_root.delete_roles(role_ids=role_names)
compass_root.delete_groups(group_names=group_names)
compass_root.delete_users(user_names=user_names)

role_mapping_role_name = "<ROLE_NAME>"
role_mapping_group_name = "<GROUP_NAME>"
compass_root.delete_role_mappings(role_name=role_mapping_role_name, group_name=role_mapping_group_name)
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
