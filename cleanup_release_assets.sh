#!/bin/bash
# Script to delete old/stale release assets from v0.1.0-alpha
# Run with: GITHUB_TOKEN=your_token bash cleanup_release_assets.sh

set -e

if [ -z "$GITHUB_TOKEN" ]; then
  echo "Error: GITHUB_TOKEN environment variable not set"
  echo "Usage: GITHUB_TOKEN=your_token bash cleanup_release_assets.sh"
  exit 1
fi

REPO="jeremyhahn/go-objstore"
RELEASE_ID="262728645"

echo "Fetching release assets..."
curl -s -H "Authorization: token $GITHUB_TOKEN" \
     -H "Accept: application/vnd.github+json" \
     "https://api.github.com/repos/${REPO}/releases/${RELEASE_ID}/assets" > /tmp/assets.json

echo "Identifying and deleting old assets..."

# Extract asset IDs and names, delete old ones
grep -E '"id"|"name"' /tmp/assets.json | \
  awk '/"id"/ && !/:/ {id=$2; gsub(/,/, "", id); getline; if ($1 == "\"name\":") {name=$2; gsub(/[",]/, "", name); print id " " name}}' | \
  while read asset_id asset_name; do
    should_delete=0

    # Delete old pattern files (NOT go-objstore-* or libobjstore-*)
    if [[ "$asset_name" =~ ^objstore-0\.1\.0-alpha- ]] || \
       [[ "$asset_name" =~ ^objstore-grpc-server- ]] || \
       [[ "$asset_name" =~ ^objstore-rest-server- ]] || \
       [[ "$asset_name" =~ ^objstore-quic-server- ]] || \
       [[ "$asset_name" =~ ^objstore-mcp-server- ]] || \
       [[ "$asset_name" =~ ^objstore-server-0\.1\.0-alpha- ]]; then
      should_delete=1
    fi

    if [ $should_delete -eq 1 ]; then
      echo "Deleting: $asset_name (ID: $asset_id)"
      curl -X DELETE \
           -H "Authorization: token $GITHUB_TOKEN" \
           -H "Accept: application/vnd.github+json" \
           "https://api.github.com/repos/${REPO}/releases/assets/${asset_id}"
      echo "  ✓ Deleted"
    fi
  done

echo ""
echo "Cleanup complete!"
echo ""
echo "Remaining assets:"
curl -s -H "Authorization: token $GITHUB_TOKEN" \
     -H "Accept: application/vnd.github+json" \
     "https://api.github.com/repos/${REPO}/releases/${RELEASE_ID}/assets" | \
  grep '"name"' | sed 's/.*"name": "\(.*\)".*/\1/' | sort
