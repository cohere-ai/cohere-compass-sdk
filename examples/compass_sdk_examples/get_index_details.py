import argparse
import json

from compass_sdk_examples.utils import get_compass_client


def parse_args():
    """
    Parse the user arguments using argparse.
    """
    parser = argparse.ArgumentParser(
        description="Get detailed configuration of a Compass index."
    )
    parser.add_argument(
        "--index-name",
        type=str,
        help="Specify the name of the index to inspect.",
        required=True,
    )

    return parser.parse_args()


def main():
    args = parse_args()
    index_name = args.index_name

    client = get_compass_client()

    try:
        print(f"Getting details for index '{index_name}'...")
        index_config = client.get_index_details(index_name=index_name)

        print(f"\nIndex Configuration for '{index_name}':")
        print("=" * 50)

        config_dict = index_config.model_dump(exclude_none=True)

        for key, value in config_dict.items():
            if value is not None:
                print(f"{key}: {value}")

        print("\nFull Configuration (JSON):")
        print(json.dumps(config_dict, indent=2))

    except Exception as e:
        print(f"Error getting index details: {e}")


if __name__ == "__main__":
    main()
