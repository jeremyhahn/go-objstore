#!/bin/bash
# Verify that all new methods are present in the SDK

set -e

echo "Verifying Python SDK has all required methods..."
echo "============================================================"

REQUIRED_METHODS=(
    "archive"
    "add_policy"
    "remove_policy"
    "get_policies"
    "apply_policies"
    "add_replication_policy"
    "remove_replication_policy"
    "get_replication_policies"
    "get_replication_policy"
    "trigger_replication"
    "get_replication_status"
)

check_client() {
    local file=$1
    local client_name=$2
    echo ""
    echo "Checking $client_name ($file)..."

    all_present=0
    for method in "${REQUIRED_METHODS[@]}"; do
        if grep -q "def $method(" "$file"; then
            echo "  ✓ $method"
        else
            echo "  ✗ $method (MISSING)"
            all_present=1
        fi
    done

    return $all_present
}

cd /home/jhahn/sources/go-objstore/api/sdks/python

overall_result=0

check_client "objstore/rest_client.py" "RestClient" || overall_result=1
check_client "objstore/grpc_client.py" "GrpcClient" || overall_result=1
check_client "objstore/quic_client.py" "QuicClient" || overall_result=1
check_client "objstore/client.py" "ObjectStoreClient (Unified)" || overall_result=1

echo ""
echo "============================================================"

if [ $overall_result -eq 0 ]; then
    echo "✓ All required methods are present!"
else
    echo "✗ Some methods are missing!"
fi

exit $overall_result
