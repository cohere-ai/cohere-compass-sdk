from cohere_compass.clients.compass import CompassClient
from cohere_compass.exceptions import CompassError

COMPASS_API_URL = "<COMPASS_API_URL>"
BEARER_TOKEN = "<BEARER_TOKEN>"

client = CompassClient(index_url=COMPASS_API_URL, bearer_token=BEARER_TOKEN)
try:
    supported = client.get_supported_file_types()
    print("Supported file types:")
    for file_type in supported.file_types:
        extensions = ", ".join(file_type.extensions) or "(no extensions)"
        mime_types = ", ".join(file_type.mime_types)
        print(f"Extensions: {extensions}, MIME types: {mime_types}")

    print(f"report.pdf supported? {supported.supports(filename='report.pdf')}")
except CompassError as e:
    print(f"Error fetching supported file types: {e}")
