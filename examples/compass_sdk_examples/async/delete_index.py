import argparse
import asyncio

from compass_sdk_examples.utils import get_compass_client_async


def parse_args():
    """
    Parse the user arguments using argparse.
    """
    parser = argparse.ArgumentParser(
        description="Delete a Compass index using async client."
    )
    parser.add_argument(
        "--index-name",
        type=str,
        help="Specify the name of the index to delete.",
        required=True,
    )

    return parser.parse_args()


async def main():
    args = parse_args()
    index_name = args.index_name

    client = get_compass_client_async()

    print(
        f"WARNING: This will permanently delete index '{index_name}' "
        "and all its documents."
    )
    confirmation = input("Are you sure you want to continue? (y/N): ")
    if confirmation.lower() not in ["y", "yes"]:
        print("Operation cancelled.")
        await client.aclose()
        return

    try:
        print(f"Deleting index '{index_name}'...")
        await client.delete_index(index_name=index_name)
        print(f"Index '{index_name}' deleted successfully.")

    except Exception as e:
        print(f"Error deleting index: {e}")
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
