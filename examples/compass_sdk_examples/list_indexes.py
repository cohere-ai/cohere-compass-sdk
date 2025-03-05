from compass_sdk_examples.utils import get_compass_api


def main():
    client = get_compass_api()
    print("Making a call to list indexes...")
    response = client.list_indexes()
    assert response.error is None
    assert response.result is not None
    indexes = response.result["indexes"]

    if not indexes:
        print("No indexes found.")
    else:
        print("Found the following indexes:")
        for idx in indexes:
            print(
                f"Index: {idx['name']}, "
                f"Documents: {idx['parent_doc_count']}, "
                f"Chunks: {idx['count']}"
            )


if __name__ == "__main__":
    main()
