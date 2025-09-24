import argparse

from compass_sdk_examples.utils import get_compass_client


def parse_args():
    """
    Parse the user arguments using argparse.
    """
    parser = argparse.ArgumentParser(
        description="Delete a specific document from a Compass index."
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
        help="Specify the ID of the document to delete.",
        required=True,
    )

    return parser.parse_args()


def main():
    args = parse_args()
    index_name = args.index_name
    document_id = args.document_id

    client = get_compass_client()

    print(
        f"WARNING: This will permanently delete document '{document_id}' "
        f"from index '{index_name}'."
    )
    confirmation = input("Are you sure you want to continue? (y/N): ")
    if confirmation.lower() not in ["y", "yes"]:
        print("Operation cancelled.")
        return

    print(f"Deleting document '{document_id}' from index '{index_name}'...")
    client.delete_document(index_name=index_name, document_id=document_id)
    print(f"Document '{document_id}' deleted successfully.")


if __name__ == "__main__":
    main()
