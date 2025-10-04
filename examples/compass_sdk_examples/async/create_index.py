import argparse
import asyncio

from compass_sdk_examples.utils import (
    get_compass_client_async,
    get_compass_parser_client,
)


def parse_args():
    """
    Parse the user arguments using argparse.
    """
    # Set up argument parsing
    parser = argparse.ArgumentParser(
        description="""
This script creates an index in Compass and inserts documents into it.
""".strip()
    )

    # Arguments
    parser.add_argument(
        "--index-name",
        type=str,
        help="Specify the name of the index to create.",
        required=True,
    )
    parser.add_argument(
        "--folder-path",
        type=str,
        help="Specify the path to the folder containing documents to insert.",
    )

    return parser.parse_args()


async def main():
    args = parse_args()
    index_name = args.index_name
    folder_path = args.folder_path

    client = get_compass_client_async()

    print(f"Creating index '{index_name}'...")
    await client.create_index(index_name=index_name)
    print(f"Index '{index_name}' created.")

    if not folder_path:
        print("No folder path provided. Skipping document insertion.")
        return

    print(f"Inserting documents from {folder_path} into index '{index_name}'...")
    parser = get_compass_parser_client()

    def process_folder():
        response = parser.process_folder(folder_path=folder_path)
        for d in response:
            if isinstance(d, tuple):
                filename, ex = d
                print(f"Failed to parse {filename}: {ex}")
            else:
                yield d

    await client.insert_docs(index_name=index_name, docs=process_folder())
    print(f"Documents inserted into index '{index_name}'.")


if __name__ == "__main__":
    asyncio.run(main())
