import argparse
import json

from compass_sdk_examples.utils import get_compass_api


def parse_args():
    """
    Parse the user arguments using argparse.
    """
    parser = argparse.ArgumentParser(
        description="""
This script retrieves all chunks from an existing index in Compass using pagination.
""".strip(),
        add_help=True,
    )

    parser.add_argument(
        "--index-name",
        type=str,
        help="Specify the name of the index to retrieve chunks from.",
        required=True,
    )
    parser.add_argument(
        "--query",
        type=str,
        help='JSON string of the query to use (default: {"match_all": {}})',
        default='{"match_all": {}}',
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Number of documents to retrieve per batch (default: 100)",
        default=100,
    )
    parser.add_argument(
        "--scroll",
        type=str,
        help="Scroll duration (default: '1m')",
        default="1m",
    )

    return parser.parse_args()


def main():
    args = parse_args()
    index_name = args.index_name

    try:
        query = json.loads(args.query)
    except json.JSONDecodeError:
        print("Error: Invalid JSON in query argument")
        return

    client = get_compass_api()

    # Inline scroll_all_chunks functionality
    if query is None:
        query = {"match_all": {}}  # type: ignore

    response = client.direct_search(
        index_name=index_name,
        query=query,  # type: ignore
        size=args.batch_size,
        scroll=args.scroll,
    )

    all_chunks = response.hits

    while response.hits and response.scroll_id:
        response = client.direct_search_scroll(
            scroll_id=response.scroll_id,
            scroll=args.scroll,
        )
        all_chunks.extend(response.hits)

    results = all_chunks

    print(f"Retrieved {len(results)} total documents")

    if results:
        print("\nPreview of first document:")
        print(results[0])


if __name__ == "__main__":
    main()
