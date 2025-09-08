import argparse

from compass_sdk_examples.utils import get_compass_client


def parse_args():
    """
    Parse the user arguments using argparse.
    """
    parser = argparse.ArgumentParser(
        description="Search chunks in a Compass index and display results."
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
        help="Search query string.",
        required=True,
    )
    parser.add_argument(
        "--top-k",
        type=int,
        help="Number of chunks to return (default: 10).",
        default=10,
    )

    return parser.parse_args()


def main():
    args = parse_args()
    index_name = args.index_name
    query = args.query
    top_k = args.top_k

    client = get_compass_client()
    print(f"Searching for '{query}' in index '{index_name}' (top {top_k} chunks)...")

    try:
        response = client.search_chunks(index_name=index_name, query=query, top_k=top_k)

        if not response.hits:
            print("No chunks found.")
            return

        print(f"Found {len(response.hits)} chunks:\n")

        for i, chunk in enumerate(response.hits, 1):
            print(f"--- Chunk {i} (Score: {chunk.score:.4f}) ---")
            print(f"Chunk ID: {chunk.chunk_id}")
            print(f"Document ID: {chunk.document_id}")
            print(f"Path: {chunk.path}")
            print(f"Content preview: {str(chunk.content)[:200]}...")
            if chunk.assets_info:
                print(f"Assets: {len(chunk.assets_info)} asset(s)")
            print()

    except Exception as e:
        print(f"Error searching chunks: {e}")


if __name__ == "__main__":
    main()
