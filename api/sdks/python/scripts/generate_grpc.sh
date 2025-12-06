#!/bin/bash
# Generate gRPC Python code from proto files

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SDK_DIR="$(dirname "$SCRIPT_DIR")"
PROTO_DIR="$PYTHON_SDK_DIR/../../proto"
OUTPUT_DIR="$PYTHON_SDK_DIR/objstore/proto"

echo "Generating gRPC Python code..."
echo "Proto directory: $PROTO_DIR"
echo "Output directory: $OUTPUT_DIR"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Generate Python gRPC code
python -m grpc_tools.protoc \
    -I"$PROTO_DIR" \
    --python_out="$OUTPUT_DIR" \
    --grpc_python_out="$OUTPUT_DIR" \
    --pyi_out="$OUTPUT_DIR" \
    "$PROTO_DIR/objstore.proto"

# Create __init__.py
touch "$OUTPUT_DIR/__init__.py"

# Fix imports in generated files
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    sed -i '' 's/^import objstore_pb2/from . import objstore_pb2/' "$OUTPUT_DIR/objstore_pb2_grpc.py"
else
    # Linux
    sed -i 's/^import objstore_pb2/from . import objstore_pb2/' "$OUTPUT_DIR/objstore_pb2_grpc.py"
fi

echo "gRPC code generation complete!"
