import argparse
import asyncio
import json

from cohere_compass.models.documents import DocumentAttributes

from compass_sdk_examples.utils import get_compass_client_async


def parse_args():
    """
    Parse the user arguments using argparse.
    """
    parser = argparse.ArgumentParser(
        description="""
        Add attributes to an existing document in a Compass index using async client.
        Attributes are added to the document's content field and become searchable.

        Examples:
          --attributes '{"category": "technical", "priority": "high"}'
          --attributes '{"tags": ["ai", "ml"], "author": "John Doe"}'
        """
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
        help="Specify the ID of the document to update.",
        required=True,
    )
    parser.add_argument(
        "--attributes",
        type=str,
        help="JSON string of attributes to add to the document.",
        required=True,
    )

    return parser.parse_args()


async def main():
    args = parse_args()
    index_name = args.index_name
    document_id = args.document_id

    try:
        attributes_dict = json.loads(args.attributes)
    except json.JSONDecodeError:
        print("Error: Invalid JSON in attributes argument")
        return

    client = get_compass_client_async()

    try:
        # Create DocumentAttributes object
        attributes = DocumentAttributes()
        for key, value in attributes_dict.items():
            setattr(attributes, key, value)

        print(
            f"Adding attributes to document '{document_id}' in index '{index_name}'..."
        )
        print(f"Attributes to add: {json.dumps(attributes_dict, indent=2)}")

        await client.add_attributes(
            index_name=index_name, document_id=document_id, attributes=attributes
        )

        print(f"Attributes added successfully to document '{document_id}'.")

    except Exception as e:
        print(f"Error adding attributes: {e}")
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
