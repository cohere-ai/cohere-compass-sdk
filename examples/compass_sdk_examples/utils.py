import os

import dotenv
from cohere.compass.clients import CompassClient, CompassParserClient

dotenv.load_dotenv()  # type: ignore


def get_compass_api():
    """
    Create and return a CompassClient instance.

    Args:
        api_base_url (str): The base URL of the Compass API.
        bearer_token (str, optional): The bearer token for auth. Defaults to None.

    Returns:
        CompassClient: An instance of CompassClient.

    """
    api_url = os.getenv("COMPASS_API_URL")
    if not api_url:
        raise ValueError(
            "COMPASS_API_URL environment variable must be set in your .env file."
        )
    bearer_token = os.getenv("COMPASS_API_BEARER_TOKEN")
    return CompassClient(index_url=api_url, bearer_token=bearer_token)


def get_compass_parser():
    """
    Create and return an instance of CompassParserClient.

    Args:
        parser_base_url (str): The base URL of the parser service.
        bearer_token (str, optional): The bearer token for auth. Defaults to None.

    Returns:
        CompassParserClient: An instance of CompassParserClient.

    """
    parser_url = os.getenv("COMPASS_PARSER_URL")
    if not parser_url:
        raise ValueError(
            "COMPASS_PARSER_URL environment variable must be set in your .env file."
        )
    bearer_token = os.getenv("COMPASS_PARSER_BEARER_TOKEN")
    return CompassParserClient(parser_url=parser_url, bearer_token=bearer_token)
