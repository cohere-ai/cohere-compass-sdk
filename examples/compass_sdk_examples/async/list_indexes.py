import asyncio

from compass_sdk_examples.utils import get_compass_client_async


async def main():
    client = get_compass_client_async()
    print("Making a call to list indexes...")

    response = await client.list_indexes()

    if not response.indexes:
        print("No indexes found.")
    else:
        print("Found the following indexes:")
        for idx in response.indexes:
            print(
                f"Index: {idx.name}, "
                f"Documents: {idx.parent_doc_count}, "
                f"Chunks: {idx.count}"
            )


if __name__ == "__main__":
    asyncio.run(main())
