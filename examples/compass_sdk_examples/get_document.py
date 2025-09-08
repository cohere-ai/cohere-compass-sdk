import argparse

from compass_sdk_examples.utils import get_compass_client


def parse_args():
    """
    Parse the user arguments using argparse.
    """
    parser = argparse.ArgumentParser(
        description="Retrieve a specific document from a Compass index."
    )
    parser.add_argument(
        "--index-name",
        type=str,
        help="Specify the name of the index.",
        required=True,
    )
    parser.add_argument(
        "--document-id",
        type=str,
        help="Specify the ID of the document to retrieve.",
        required=True,
    )

    return parser.parse_args()


def main():
    args = parse_args()
    index_name = args.index_name
    document_id = args.document_id

    client = get_compass_client()

    try:
        print(f"Retrieving document '{document_id}' from index '{index_name}'...")
        result = client.get_document(index_name=index_name, document_id=document_id)

        print(result.model_dump_json(indent=2))

    except Exception as e:
        print(f"Error retrieving document: {e}")


if __name__ == "__main__":
    main()
