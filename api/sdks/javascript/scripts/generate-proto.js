import { execSync } from 'child_process';
import { mkdirSync, existsSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const PROTO_DIR = join(__dirname, '../../../proto');
const OUT_DIR = join(__dirname, '../src/proto');

// Create output directory
if (!existsSync(OUT_DIR)) {
  mkdirSync(OUT_DIR, { recursive: true });
}

console.log('Generating protobuf JavaScript code...');
console.log('Proto dir:', PROTO_DIR);
console.log('Output dir:', OUT_DIR);

try {
  // Generate JavaScript code using grpc-tools
  const cmd = `npx grpc_tools_node_protoc \
    --js_out=import_style=commonjs,binary:${OUT_DIR} \
    --grpc_out=grpc_js:${OUT_DIR} \
    --proto_path=${PROTO_DIR} \
    ${PROTO_DIR}/objstore.proto`;

  console.log('Running:', cmd);
  execSync(cmd, { stdio: 'inherit' });

  console.log('Protobuf code generated successfully!');
} catch (error) {
  console.error('Failed to generate protobuf code:', error.message);
  process.exit(1);
}
