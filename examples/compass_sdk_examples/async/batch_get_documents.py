import argparse
import asyncio

from compass_sdk_examples.utils import get_compass_client_async


def parse_args():
    """
    Parse the user arguments using argparse.
    """
    parser = argparse.ArgumentParser(
        description="Retrieve multiple documents from a Compass index in a single request."
    )
    parser.add_argument(
        "--index-name",
        type=str,
        help="Specify the name of the index.",
        required=True,
    )
    parser.add_argument(
        "--document-ids",
        type=str,
        nargs="+",
        help="One or more document IDs to retrieve.",
        required=True,
    )

    return parser.parse_args()


async def main():
    args = parse_args()
    index_name = args.index_name
    document_ids = args.document_ids

    client = get_compass_client_async()

    try:
        print(
            f"Retrieving {len(document_ids)} document(s) from index '{index_name}'..."
        )
        result = await client.batch_get_documents(
            index_name=index_name, document_ids=document_ids
        )

        print(f"Found {len(result.documents)} document(s):\n")
        for doc in result.documents:
            print(doc.model_dump_json(indent=2))

    except Exception as e:
        import traceback

        traceback.print_exc()
        print(f"Error retrieving documents: {e}")
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
