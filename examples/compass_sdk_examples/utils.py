import os

import dotenv
from cohere_compass.clients import (
    CompassAsyncClient,
    CompassClient,
    CompassParserAsyncClient,
    CompassParserClient,
)
from cohere_compass.models import ParserConfig

dotenv.load_dotenv()  # type: ignore


def get_compass_client():
    """
    Create and return a CompassClient instance.
    """
    api_url = os.getenv("COMPASS_API_URL")
    if not api_url:
        raise ValueError(
            "COMPASS_API_URL environment variable must be set in your .env file."
        )
    bearer_token = os.getenv("COMPASS_API_BEARER_TOKEN")
    return CompassClient(index_url=api_url, bearer_token=bearer_token)


def get_compass_client_async():
    """
    Create and return an instance of CompassAsyncClient.
    """
    api_url = os.getenv("COMPASS_API_URL")
    if not api_url:
        raise ValueError(
            "COMPASS_API_URL environment variable must be set in your .env file."
        )
    bearer_token = os.getenv("COMPASS_API_BEARER_TOKEN")
    return CompassAsyncClient(index_url=api_url, bearer_token=bearer_token)


def get_compass_parser_client(parser_config: ParserConfig = ParserConfig()):
    """
    Create and return an instance of CompassParserClient.
    """
    parser_url = os.getenv("COMPASS_PARSER_URL")
    if not parser_url:
        raise ValueError(
            "COMPASS_PARSER_URL environment variable must be set in your .env file."
        )
    bearer_token = os.getenv("COMPASS_PARSER_BEARER_TOKEN")
    return CompassParserClient(
        parser_url=parser_url,
        bearer_token=bearer_token,
        parser_config=parser_config,
    )


def get_compass_parser_async_client(parser_config: ParserConfig = ParserConfig()):
    """
    Create and return an instance of CompassParserClient.
    """
    parser_url = os.getenv("COMPASS_PARSER_URL")
    if not parser_url:
        raise ValueError(
            "COMPASS_PARSER_URL environment variable must be set in your .env file."
        )
    bearer_token = os.getenv("COMPASS_PARSER_BEARER_TOKEN")
    return CompassParserAsyncClient(
        parser_url=parser_url,
        bearer_token=bearer_token,
        parser_config=parser_config,
    )
