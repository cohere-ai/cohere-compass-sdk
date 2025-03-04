from compass_sdk_examples.utils import get_compass_client


def main():
    client = get_compass_client()
    response = client.list_indexes()
    print(response)


if __name__ == "__main__":
    main()
