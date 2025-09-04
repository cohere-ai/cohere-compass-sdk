from compass_sdk_examples.utils import get_compass_api


def main():
    client = get_compass_api()

    print(client.get_models())


if __name__ == "__main__":
    main()
