from cohere_compass.clients.compass import CompassClient
from cohere_compass.exceptions import CompassError

COMPASS_API_URL = ...
BEARER_TOKEN = ...
INDEX_NAME = ...

client = CompassClient(index_url=COMPASS_API_URL, bearer_token=BEARER_TOKEN)
try:
    details = client.get_index_details(index_name=INDEX_NAME)
    print(f"Index details: {details}")
except CompassError as e:
    print(f"Error fetching models: {e}")
