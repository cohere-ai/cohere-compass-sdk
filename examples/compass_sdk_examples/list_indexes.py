from compass_sdk_examples.utils import get_compass_api


def main():
    client = get_compass_api()
    print("Making a call to list indexes...")
    response = client.list_indexes()
    if response.error:
        print(f"Error: {response.error}")
        return
    if not response.result:
        print("Unexpected error: request didn't return any result.")
        return
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
