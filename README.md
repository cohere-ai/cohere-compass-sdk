# Cohere Compass SDK 

The Compass SDK is a Python library that allows you to parse documents and insert them into a Compass index.

In order to parse documents, the Compass SDK relies on the Compass Parser API, which is a RESTful API that
receives files and returns parsed documents. This requires a hosted Compass server.

The Compass SDK provides a `CompassParserClient` that allows to interact with the parser API from your
Python code in a convenient manner. The `CompassParserClient` provides methods to parse single and multiple
files, as well as entire folders, and supports multiple file types (e.g., `pdf`, `docx`, `json`, `csv`, etc.) as well
as different file systems (e.g., local, S3, GCS, etc.).

To insert parsed documents into a `Compass` index, the Compass SDK provides a `CompassClient` class that
allows to interact with a Compass API server. The Compass API is also a RESTful API that allows to create,
delete and search documents in a Compass index. To install a Compass API service, please refer to the
[Compass documentation](https://github.com/cohere-ai/compass)

## Quickstart Snippet 

Fill in your URL, username, password, and path to test data below for an end to end run of parsing and searching. 

```
from compass_sdk.compass import CompassClient
from compass_sdk.parser import CompassParserClient
from compass_sdk import MetadataStrategy, MetadataConfig

# Using cohere_web_test folder for data
url = "<COMPASS_URL>"
username = "<COMPASS_USERNAME>" 
password = "<COMPASS_PASSWORD>"

index = "test-index"
data_to_index = "<PATH_TO_TEST_DATA>"


# Parse the files before indexing
parser_url = url + '/parse'
parsing_client = CompassParserClient(parser_url = parser_url)
metadata_config = MetadataConfig(
    metadata_strategy=MetadataStrategy.Command_R,
    commandr_extractable_attributes=["date", "link", "page_title", "authors"]
)

docs_to_index = parsing_client.process_folder(folder_path=data_to_index, metadata_config=metadata_config)

# Create index and insert files
compass_client = CompassClient(index_url=url)
compass_client.create_index(index_name=index)
results = compass_client.insert_docs(index_name=index, docs=docs_to_index)

results = compass_client.search(index_name=index, query="test", top_k=1)
print(f"Results preview: \n {results.result['hits'][-1]} ... \n \n ") 
```
