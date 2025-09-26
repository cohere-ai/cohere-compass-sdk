from cohere_compass.clients.compass import CompassClient
from cohere_compass.exceptions import CompassError

COMPASS_API_URL = "<COMPASS_API_URL>"
BEARER_TOKEN = "<BEARER_TOKEN>"

client = CompassClient(index_url=COMPASS_API_URL, bearer_token=BEARER_TOKEN)
try:
    models = client.get_models()
    print("Available models:")
    for role, versions in models.items():
        print(f"Role: {role}, Versions: {versions}")
except CompassError as e:
    print(f"Error fetching models: {e}")
