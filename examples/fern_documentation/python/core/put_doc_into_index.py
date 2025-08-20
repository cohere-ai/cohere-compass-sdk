from cohere_compass.clients import CompassClient, CompassParserClient

COMPASS_API_URL = ...
PARSER_URL = ...
BEARER_TOKEN = ...
INDEX_NAME = ...
FOLDER_TO_PROCESS = ...

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)

compass_parser_client = CompassParserClient(parser_url=PARSER_URL)

docs_to_index = compass_parser_client.process_folder(
    folder_path=FOLDER_TO_PROCESS, recursive=True
)

compass_client.create_index(index_name=INDEX_NAME)
r = compass_client.insert_docs(index_name=INDEX_NAME, docs=docs_to_index, num_jobs=1)

print(r)
