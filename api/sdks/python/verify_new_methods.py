#!/usr/bin/env python3
"""Verify that all new methods are present in the SDK."""

import inspect
from objstore.client import ObjectStoreClient
from objstore.rest_client import RestClient
from objstore.grpc_client import GrpcClient
from objstore.quic_client import QuicClient

# Methods that should be present in all clients
REQUIRED_METHODS = [
    "archive",
    "add_policy",
    "remove_policy",
    "get_policies",
    "apply_policies",
    "add_replication_policy",
    "remove_replication_policy",
    "get_replication_policies",
    "get_replication_policy",
    "trigger_replication",
    "get_replication_status",
]


def check_methods(client_class, client_name):
    """Check if all required methods are present in a client."""
    print(f"\nChecking {client_name}...")
    methods = [m for m in dir(client_class) if not m.startswith("_")]
    missing = []

    for method in REQUIRED_METHODS:
        if method not in methods:
            missing.append(method)
        else:
            print(f"  ✓ {method}")

    if missing:
        print(f"  ✗ Missing methods: {', '.join(missing)}")
        return False

    return True


def main():
    """Main verification function."""
    print("Verifying Python SDK has all required methods...")
    print("=" * 60)

    all_present = True

    # Check each client
    all_present &= check_methods(RestClient, "RestClient")
    all_present &= check_methods(GrpcClient, "GrpcClient")
    all_present &= check_methods(QuicClient, "QuicClient")
    all_present &= check_methods(ObjectStoreClient, "ObjectStoreClient (Unified)")

    print("\n" + "=" * 60)

    if all_present:
        print("✓ All required methods are present!")
        return 0
    else:
        print("✗ Some methods are missing!")
        return 1


if __name__ == "__main__":
    exit(main())
