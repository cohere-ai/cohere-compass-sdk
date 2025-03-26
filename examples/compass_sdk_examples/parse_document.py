import argparse
import json

from cohere_compass.models import ParserConfig, PDFParsingStrategy

from compass_sdk_examples.utils import get_compass_parser


def parse_args():
    """
    Parse the user arguments using argparse.
    """
    # Set up argument parsing
    parser = argparse.ArgumentParser(
        description="This script parses a file using Compass Parser."
    )

    # Arguments
    parser.add_argument(
        "--file",
        type=str,
        help="Specify the name of the file to parse.",
        required=True,
    )

    return parser.parse_args()


def main():
    args = parse_args()

    parser = get_compass_parser()
    docs = parser.process_file(
        filename=args.file,
        parser_config=ParserConfig(
            pdf_parsing_strategy=PDFParsingStrategy.ImageToMarkdown
        ),
    )
    for doc in docs:
        print(json.dumps(doc.content, indent=2, default=str))


if __name__ == "__main__":
    main()
