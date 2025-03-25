import json

from cohere_compass.clients import CompassClient

COMPASS_API_URL = ...
BEARER_TOKEN = ...
INDEX_NAME = ...
DOCUMENT_ID = ...
SEARCH_QUERY = ...

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)

results = compass_client.search_chunks(index_name=INDEX_NAME, query=SEARCH_QUERY)
if results.error:
    raise Exception(f"Failed to search chunks: {results.error}")

hits = results.hits

if not hits:
    raise Exception("No hits found")

hit = hits[0]

asset_id: str = hit.assets_info[0].asset_id  # type: ignore
document_id = hit.document_id
# URL to fetch asset
presigned_url = hit.assets_info[0].presigned_url

### Get document asset again, after pre-signed url has expired
asset, content_type = compass_client.get_document_asset(
    index_name=document_id,
    document_id=DOCUMENT_ID,
    asset_id=asset_id,
)

# Save the asset to a file.
if content_type in ["image/jpeg", "image/png"]:
    with open(f"{asset_id}", "wb") as f:
        f.write(asset)
# Print the asset as JSON
elif "text/json" in content_type:
    print(json.dumps(asset, indent=2))
