from compass_sdk_examples.utils import get_compass_api


def main():
    client = get_compass_api()
    # feel free to change the index name and query.
    response = client.search_documents(index_name="cohere-papers", query="test")
    print(response.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
