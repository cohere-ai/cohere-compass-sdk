import argparse
import asyncio
import json

from cohere_compass.models import RetentionPolicy, RetentionType

from compass_sdk_examples.utils import get_compass_client_async


def parse_args():
    """
    Parse the user arguments using argparse.
    """
    parser = argparse.ArgumentParser(
        description="""
Manage retention policies for a Compass index using async client.

Retention policies automatically soft-delete and purge documents after a defined period.
- Fixed: Documents expire based on created_at + ttl_days
- Sliding: Documents expire based on accessed_at + ttl_days (documents that keep
  getting accessed are never deleted)
""".strip()
    )
    parser.add_argument(
        "--index-name",
        type=str,
        help="Specify the name of the index.",
        required=True,
    )

    subparsers = parser.add_subparsers(dest="action", help="Action to perform")

    # Set retention policy
    set_parser = subparsers.add_parser("set", help="Set a retention policy")
    set_parser.add_argument(
        "--type",
        type=str,
        choices=["fixed", "sliding"],
        required=True,
        help="Retention type: 'fixed' (based on creation time) or 'sliding' (based on last access).",
    )
    set_parser.add_argument(
        "--ttl-days",
        type=int,
        required=True,
        help="Time-to-live in days. Documents older than this will be soft-deleted.",
    )
    set_parser.add_argument(
        "--grace-period-days",
        type=int,
        default=7,
        help="Days between soft-delete and hard-delete (default: 7).",
    )
    set_parser.add_argument(
        "--disabled",
        action="store_true",
        help="Set the policy as disabled (default: enabled).",
    )

    # Get retention policy
    subparsers.add_parser("get", help="Get the current retention policy")

    # Delete retention policy
    subparsers.add_parser("delete", help="Remove the retention policy")

    return parser.parse_args()


async def main():
    args = parse_args()
    index_name = args.index_name

    client = get_compass_client_async()

    try:
        if args.action == "set":
            retention_type = RetentionType.Fixed if args.type == "fixed" else RetentionType.Sliding

            policy = RetentionPolicy(
                retention_type=retention_type,
                ttl_days=args.ttl_days,
                grace_period_days=args.grace_period_days,
                enabled=not args.disabled,
            )

            print(f"Setting retention policy for index '{index_name}'...")
            print(f"  Type: {policy.retention_type.value}")
            print(f"  TTL: {policy.ttl_days} days")
            print(f"  Grace period: {policy.grace_period_days} days")
            print(f"  Enabled: {policy.enabled}")

            await client.set_retention_policy(index_name=index_name, retention_policy=policy)
            print("\nRetention policy set successfully.")

        elif args.action == "get":
            print(f"Getting retention policy for index '{index_name}'...")

            policy = await client.get_retention_policy(index_name=index_name)

            if policy is None:
                print(f"\nNo retention policy configured for index '{index_name}'.")
            else:
                print(f"\nRetention Policy for '{index_name}':")
                print("=" * 50)
                print(f"Type: {policy.retention_type.value}")
                print(f"TTL: {policy.ttl_days} days")
                print(f"Grace period: {policy.grace_period_days} days")
                print(f"Enabled: {policy.enabled}")
                print("\nFull Policy (JSON):")
                print(json.dumps(policy.model_dump(), indent=2))

        elif args.action == "delete":
            print(f"Removing retention policy from index '{index_name}'...")
            await client.delete_retention_policy(index_name=index_name)
            print("Retention policy removed successfully.")

        else:
            print("Please specify an action: set, get, or delete")
            print("Use --help for more information.")

    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
