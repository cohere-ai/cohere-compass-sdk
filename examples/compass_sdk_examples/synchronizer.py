import argparse
import json

from cohere_compass.models import UpsertSynchronizerRequest

from compass_sdk_examples.utils import get_compass_client


def parse_args():
    """
    Parse the user arguments using argparse.
    """
    parser = argparse.ArgumentParser(
        description="""
Manage synchronizers for a Compass index.

A synchronizer binds an index to an external data origin (e.g. SharePoint, OneDrive)
and lets Atlas keep the index in sync. The typical flow is:

  1. origins              -> discover available data origins
  2. create               -> create a synchronizer
  3. auth-url             -> get the OAuth URL and authorize in a browser
  4. sync                 -> start a sync job
  5. status               -> poll cumulative sync status
""".strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--index-name", type=str, required=True, help="Name of the index."
    )
    parser.add_argument("--name", type=str, help="Name of the synchronizer.")

    subparsers = parser.add_subparsers(dest="action", help="Action to perform")

    subparsers.add_parser("origins", help="List supported data origins")

    create_parser = subparsers.add_parser(
        "create", help="Create or update a synchronizer"
    )
    create_parser.add_argument(
        "--data-origin-id", type=str, required=True, help="e.g. 'sharepoint'."
    )
    create_parser.add_argument(
        "--credential-bundle-id",
        type=str,
        default="user-default",
        help="Credential bundle identifier (default: user-default).",
    )

    subparsers.add_parser("get", help="Get a synchronizer")
    subparsers.add_parser("list", help="List all synchronizers for the index")
    subparsers.add_parser("delete", help="Delete a synchronizer")
    subparsers.add_parser("auth-url", help="Get the OAuth authorization URL")
    subparsers.add_parser("sync", help="Start a sync job")
    subparsers.add_parser("status", help="Get cumulative sync status")

    return parser.parse_args()


def _require_name(args: argparse.Namespace) -> str:
    if not args.name:
        raise SystemExit("--name is required for this action.")
    return args.name


def main():
    args = parse_args()
    index_name = args.index_name
    client = get_compass_client()

    if args.action == "origins":
        origins = client.list_data_origins().data_origins
        print("Supported data origins:")
        for origin in origins:
            print(f"  {origin.id}: {origin.name}")

    elif args.action == "create":
        name = _require_name(args)
        synchronizer = client.upsert_synchronizer(
            index_name=index_name,
            synchronizer_name=name,
            synchronizer=UpsertSynchronizerRequest(
                data_origin_id=args.data_origin_id,
                credential_bundle_id=args.credential_bundle_id,
            ),
        )
        print(
            f"Created synchronizer '{synchronizer.name}' "
            f"(auth_status={synchronizer.auth_status})."
        )
        print("Next: run 'auth-url' to authorize, then 'sync' to start syncing.")

    elif args.action == "get":
        synchronizer = client.get_synchronizer(
            index_name=index_name, synchronizer_name=_require_name(args)
        )
        print(json.dumps(synchronizer.model_dump(mode="json"), indent=2))

    elif args.action == "list":
        synchronizers = client.list_synchronizers(index_name=index_name).synchronizers
        for synchronizer in synchronizers:
            print(
                f"  {synchronizer.name}: origin={synchronizer.data_origin_id} "
                f"auth={synchronizer.auth_status}"
            )

    elif args.action == "delete":
        client.delete_synchronizer(
            index_name=index_name, synchronizer_name=_require_name(args)
        )
        print("Synchronizer deleted.")

    elif args.action == "auth-url":
        oauth = client.get_synchronizer_oauth_url(
            index_name=index_name, synchronizer_name=_require_name(args)
        )
        print("Open this URL in a browser to authorize the data origin:")
        print(oauth.auth_url)

    elif args.action == "sync":
        client.start_sync(index_name=index_name, synchronizer_name=_require_name(args))
        print("Sync job started.")

    elif args.action == "status":
        name = _require_name(args)
        status = client.get_sync_status(
            index_name=index_name, synchronizer_name=name
        ).sync_status
        if status is None:
            print("No sync status available yet.")
        else:
            print(
                f"Progress: {status.done_count}/{status.total_count} "
                f"(state={status.last_sync_state})"
            )
            for item in status.failed_items:
                print(f"  failed: {item.data_id} - {item.error_message}")

    else:
        print("Please specify an action. Use --help for more information.")


if __name__ == "__main__":
    main()
