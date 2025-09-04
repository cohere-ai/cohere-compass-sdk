from compass_sdk_examples.utils import get_compass_client, get_compass_client_async


def main():
    client = get_compass_client()

    print(client.get_models())


async def main_async():
    client = get_compass_client_async()

    print(await client.get_models())


if __name__ == "__main__":
    main()

    # Or use the async version...
    # import asyncio
    # asyncio.run(main_async())
