#!/usr/bin/env python3
"""Example demonstrating lifecycle and replication operations.

This example shows how to use the new lifecycle and replication features
added to the go-objstore Python SDK.
"""

from objstore import (
    ObjectStoreClient,
    LifecyclePolicy,
    ReplicationPolicy,
    TriggerReplicationOptions,
    Protocol,
)


def lifecycle_example():
    """Demonstrate lifecycle policy operations."""
    print("=" * 60)
    print("Lifecycle Policy Operations Example")
    print("=" * 60)

    # Create client
    client = ObjectStoreClient(
        protocol=Protocol.REST,
        base_url="http://localhost:8080",
    )

    try:
        # 1. Add a lifecycle policy to delete old logs
        print("\n1. Adding lifecycle policy to delete old logs...")
        delete_policy = LifecyclePolicy(
            id="delete-old-logs",
            prefix="logs/",
            retention_seconds=604800,  # 7 days
            action="delete",
        )
        result = client.add_policy(delete_policy)
        print(f"   Result: {result.message}")

        # 2. Add a policy to archive data after 30 days
        print("\n2. Adding lifecycle policy to archive old data...")
        archive_policy = LifecyclePolicy(
            id="archive-old-data",
            prefix="data/",
            retention_seconds=2592000,  # 30 days
            action="archive",
            destination_type="s3",
            destination_settings={
                "bucket": "archive-bucket",
                "region": "us-west-2",
            },
        )
        result = client.add_policy(archive_policy)
        print(f"   Result: {result.message}")

        # 3. List all policies
        print("\n3. Listing all lifecycle policies...")
        policies_response = client.get_policies()
        for policy in policies_response.policies:
            print(f"   - {policy.id}: {policy.action} objects with prefix '{policy.prefix}'")
            print(f"     Retention: {policy.retention_seconds} seconds")

        # 4. Apply policies immediately
        print("\n4. Applying lifecycle policies...")
        apply_result = client.apply_policies()
        print(f"   Policies applied: {apply_result.policies_count}")
        print(f"   Objects processed: {apply_result.objects_processed}")

        # 5. Remove a policy
        print("\n5. Removing a lifecycle policy...")
        result = client.remove_policy("delete-old-logs")
        print(f"   Result: {result.message}")

    finally:
        client.close()


def replication_example():
    """Demonstrate replication policy operations."""
    print("\n" + "=" * 60)
    print("Replication Policy Operations Example")
    print("=" * 60)

    # Create client
    client = ObjectStoreClient(
        protocol=Protocol.REST,
        base_url="http://localhost:8080",
    )

    try:
        # 1. Add a replication policy
        print("\n1. Adding replication policy (local -> S3)...")
        policy = ReplicationPolicy(
            id="local-to-s3",
            source_backend="local",
            source_settings={"path": "/data"},
            source_prefix="important/",
            destination_backend="s3",
            destination_settings={
                "bucket": "backup-bucket",
                "region": "us-east-1",
            },
            check_interval_seconds=300,  # Check every 5 minutes
            enabled=True,
        )
        result = client.add_replication_policy(policy)
        print(f"   Result: {result.message}")

        # 2. List all replication policies
        print("\n2. Listing all replication policies...")
        policies_response = client.get_replication_policies()
        for policy in policies_response.policies:
            status = "enabled" if policy.enabled else "disabled"
            print(
                f"   - {policy.id}: {policy.source_backend} -> "
                f"{policy.destination_backend} ({status})"
            )

        # 3. Get a specific replication policy
        print("\n3. Getting specific replication policy...")
        policy = client.get_replication_policy("local-to-s3")
        print(f"   Policy ID: {policy.id}")
        print(f"   Source: {policy.source_backend}")
        print(f"   Destination: {policy.destination_backend}")
        print(f"   Interval: {policy.check_interval_seconds}s")

        # 4. Trigger replication manually
        print("\n4. Triggering replication manually...")
        opts = TriggerReplicationOptions(
            policy_id="local-to-s3",
            parallel=True,
            worker_count=4,
        )
        trigger_result = client.trigger_replication(opts)

        if trigger_result.success and trigger_result.result:
            sync_result = trigger_result.result
            print(f"   Synced: {sync_result.synced} objects")
            print(f"   Deleted: {sync_result.deleted} objects")
            print(f"   Failed: {sync_result.failed} objects")
            print(f"   Bytes transferred: {sync_result.bytes_total}")
            print(f"   Duration: {sync_result.duration_ms}ms")

            if sync_result.errors:
                print(f"   Errors: {len(sync_result.errors)}")
                for error in sync_result.errors[:3]:  # Show first 3 errors
                    print(f"     - {error}")

        # 5. Get replication status
        print("\n5. Getting replication status...")
        status_response = client.get_replication_status("local-to-s3")

        if status_response.success and status_response.status:
            status = status_response.status
            print(f"   Policy ID: {status.policy_id}")
            print(f"   Total objects synced: {status.total_objects_synced}")
            print(f"   Total objects deleted: {status.total_objects_deleted}")
            print(f"   Total bytes synced: {status.total_bytes_synced}")
            print(f"   Total errors: {status.total_errors}")
            print(f"   Sync count: {status.sync_count}")
            print(f"   Average sync duration: {status.average_sync_duration_ms}ms")
            if status.last_sync_time:
                print(f"   Last sync: {status.last_sync_time}")

        # 6. Remove replication policy
        print("\n6. Removing replication policy...")
        result = client.remove_replication_policy("local-to-s3")
        print(f"   Result: {result.message}")

    finally:
        client.close()


def archive_example():
    """Demonstrate archive operation."""
    print("\n" + "=" * 60)
    print("Archive Operation Example")
    print("=" * 60)

    # Create client
    client = ObjectStoreClient(
        protocol=Protocol.REST,
        base_url="http://localhost:8080",
    )

    try:
        # Archive an object to S3
        print("\n1. Archiving object to S3...")
        result = client.archive(
            key="data/important-file.dat",
            destination_type="s3",
            settings={
                "bucket": "archive-bucket",
                "region": "us-west-2",
                "storage_class": "GLACIER",
            },
        )
        print(f"   Result: {result.message}")

        # Archive with GCS
        print("\n2. Archiving object to Google Cloud Storage...")
        result = client.archive(
            key="data/backup.tar.gz",
            destination_type="gcs",
            settings={
                "bucket": "archive-bucket",
                "project": "my-project",
            },
        )
        print(f"   Result: {result.message}")

    finally:
        client.close()


def main():
    """Main function."""
    print("\ngo-objstore Python SDK - Lifecycle & Replication Examples\n")

    try:
        # Run examples
        lifecycle_example()
        replication_example()
        archive_example()

        print("\n" + "=" * 60)
        print("All examples completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\nError running examples: {e}")
        print("\nNote: These examples require a running go-objstore server.")
        print("Start the server with: make run")


if __name__ == "__main__":
    main()
