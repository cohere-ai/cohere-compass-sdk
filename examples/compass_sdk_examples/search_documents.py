import argparse

from compass_sdk_examples.utils import get_compass_api


def parse_args():
    """
    Parse the user arguments using argparse.
    """
    # Set up argument parsing
    parser = argparse.ArgumentParser(
        description="""
This script searches for documents in an existing index in Compass.
""".strip(),
        add_help=True,
    )

    # Arguments
    parser.add_argument(
        "--index-name",
        type=str,
        help="Specify the name of the index to search in.",
        required=True,
    )
    parser.add_argument(
        "--query",
        type=str,
        help="Specify the query to search for.",
        required=True,
    )

    return parser.parse_args()


def main():
    args = parse_args()
    index_name = args.index_name
    query = args.query

    client = get_compass_api()
    # feel free to change the index name and query.
    response = client.search_documents(index_name=index_name, query=query)
    print(response.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
