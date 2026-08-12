from cohere_compass.models import SupportedFileTypesResponse

from compass_sdk_examples.utils import get_compass_client, get_compass_client_async


def print_supported_file_types(supported: SupportedFileTypesResponse) -> None:
    print("Supported file types:")
    for file_type in supported.file_types:
        extensions = ", ".join(file_type.extensions) or "(no extensions)"
        print(f"  {extensions}: {', '.join(file_type.mime_types)}")


def main():
    client = get_compass_client()

    supported = client.get_supported_file_types()
    print_supported_file_types(supported)

    # The supported set depends on the deployment's runtime configuration, so audio
    # only appears when ASR is enabled on the Compass being queried.
    print(f"\nreport.pdf supported? {supported.supports(filename='report.pdf')}")
    print(f"podcast.mp3 supported? {supported.supports(filename='podcast.mp3')}")


async def main_async():
    client = get_compass_client_async()

    print_supported_file_types(await client.get_supported_file_types())


if __name__ == "__main__":
    main()

    # Or use the async version...
    # import asyncio
    # asyncio.run(main_async())
