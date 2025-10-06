import argparse
import json

from compass_sdk_examples.utils import get_compass_client


def parse_args():
    """
    Parse the user arguments using argparse.
    """
    parser = argparse.ArgumentParser(
        description="""
        Perform a direct search using Elasticsearch-style queries.
        Examples:
          Match all: '{"match_all": {}}'
          Term query: '{"term": {"content.title": "example"}}'
          Range query: '{"range": {"sort_id": {"gte": 0, "lte": 100}}}'
        """
    )
    parser.add_argument(
        "--index-name",
        type=str,
        help="Specify the name of the index to search.",
        required=True,
    )
    parser.add_argument(
        "--query",
        type=str,
        help="Elasticsearch-style query in JSON format.",
        default='{"match_all": {}}',
    )
    parser.add_argument(
        "--size",
        type=int,
        help="Number of results to return (default: 10).",
        default=3,
    )
    parser.add_argument(
        "--sort-field",
        type=str,
        help="Field to sort by (optional).",
        default=None,
    )
    parser.add_argument(
        "--sort-order",
        type=str,
        choices=["asc", "desc"],
        help="Sort order (default: desc).",
        default="desc",
    )

    return parser.parse_args()


def main():
    args = parse_args()
    index_name = args.index_name
    size = args.size
    sort_field = args.sort_field
    sort_order = args.sort_order

    try:
        query = json.loads(args.query)
    except json.JSONDecodeError:
        print("Error: Invalid JSON in query argument")
        return

    client = get_compass_client()

    try:
        print(f"Performing direct search on index '{index_name}'...")
        print(f"Query: {json.dumps(query, indent=2)}")

        # Prepare sort_by parameter if sort field is specified
        sort_by = None
        if sort_field:
            from cohere_compass.models.search import SortBy

            sort_by = [SortBy(field=sort_field, order=sort_order)]
            print(f"Sorting by: {sort_field} ({sort_order})")

        response = client.direct_search(
            index_name=index_name,
            query=query,
            sort_by=sort_by,
            size=size,
            scroll="1m",
        )

        for _ in range(3):  # up to 3 scrolls to avoid lots of results.
            if not response.hits:
                print("No results found.")
                break

            print(f"\nFound {len(response.hits)} results:")
            print("=" * 50)

            for i, hit in enumerate(response.hits, 1):
                print(f"\n--- Result {i} (Score: {hit.score:.4f}) ---")
                print(f"Chunk ID: {hit.chunk_id}")
                print(f"Document ID: {hit.document_id}")
                print(f"Path: {hit.path}")
                print(f"Sort ID: {hit.sort_id}")

                # Show content preview
                content_str = str(hit.content)[:200]
                print(f"Content preview: {content_str}...")

                if hit.assets_info:
                    print(f"Assets: {len(hit.assets_info)} asset(s)")

            if response.scroll_id:
                print(f"\nScroll ID for next page: {response.scroll_id}")
                response = client.direct_search_scroll(
                    scroll_id=response.scroll_id,
                    index_name=index_name,
                    scroll="1m",
                )
            else:
                break

    except Exception as e:
        print(f"Error performing direct search: {e}")


if __name__ == "__main__":
    main()
