from cohere_compass import GroupAuthorizationInput
from cohere_compass.clients import CompassClient

COMPASS_API_URL = "<COMPASS_API_URL>"
BEARER_TOKEN = "<BEARER_TOKEN>"
INDEX_NAME = "<INDEX_NAME>"
DOCUMENT_ID = "<DOCUMENT_ID>"
AUTHORIZED_GROUP_NAME = "<AUTHORIZED_GROUP_NAME>"

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)

try:
    r = compass_client.update_group_authorization(
        index_name=INDEX_NAME,
        group_auth_input=GroupAuthorizationInput(
            document_ids=[DOCUMENT_ID],
            authorized_groups=[AUTHORIZED_GROUP_NAME],
            action=GroupAuthorizationInput.Actions.ADD,
        ),
    )
except Exception as e:
    raise Exception(f"Failed to update group auth: {e}")

for doc in r.results:
    if doc.error:
        print(f"Error processing doc: {doc.document_id}, error: {doc.error}")
        continue
    print(f"Successfully processed doc: {doc.document_id}")
