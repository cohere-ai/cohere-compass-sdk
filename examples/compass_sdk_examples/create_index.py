from compass_sdk_examples.utils import get_compass_api, get_compass_parser


def main():
    client = get_compass_api()

    print("Creating index 'cohere-papers'...")
    client.create_index(index_name="cohere-papers")
    print("Index 'cohere-papers' created.")

    print("Inserting documents into index 'cohere-papers'...")
    parser = get_compass_parser()
    docs = parser.process_folder(folder_path="./sample_docs")
    client.insert_docs(index_name="cohere-papers", docs=iter(docs))
    print("Documents inserted into index 'cohere-papers'.")


if __name__ == "__main__":
    main()
