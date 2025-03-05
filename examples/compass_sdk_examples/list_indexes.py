from compass_sdk_examples.utils import get_compass_api


def main():
    client = get_compass_api()
    print("Making a call to list indexes...")
    response = client.list_indexes()
    print("Printing response...")
    print(response)


if __name__ == "__main__":
    main()
