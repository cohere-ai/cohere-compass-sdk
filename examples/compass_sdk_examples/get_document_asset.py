import argparse
import json

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
    response = client.search_chunks(index_name=index_name, query=query)
    if not response.hits:
        print("No hits found.")
        return

    # Get the first hit that has some assets.
    first_hit_with_assets = next(
        (hit for hit in response.hits if hit.assets_info),
        None,
    )
    if not first_hit_with_assets:
        print("No hits with assets found.")
        return

    # Retrieve the ID of the first asset in the first hit.
    asset_id: str = first_hit_with_assets.assets_info[1].asset_id  # type: ignore
    document_id = first_hit_with_assets.document_id

    # Get the asset.
    asset, content_type = client.get_document_asset(
        index_name=index_name,
        document_id=document_id,
        asset_id=asset_id,  # type: ignore
    )

    # Save the asset to a file.
    if content_type in ["image/jpeg", "image/png"]:
        with open(f"{asset_id}", "wb") as f:
            f.write(asset)  # type: ignore
    elif content_type is not None and "text/json" in content_type:
        print(json.dumps(asset, indent=2))


if __name__ == "__main__":
    main()
