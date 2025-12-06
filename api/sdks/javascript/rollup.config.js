import resolve from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';
import json from '@rollup/plugin-json';

export default [
  // ESM build
  {
    input: 'src/index.js',
    output: {
      file: 'dist/index.js',
      format: 'esm',
      sourcemap: true,
    },
    external: ['@grpc/grpc-js', '@grpc/proto-loader', 'axios', 'form-data', 'google-protobuf', 'fs', 'path', 'http2'],
    plugins: [
      resolve({ preferBuiltins: true }),
      commonjs(),
      json(),
    ],
  },
  // CommonJS build
  {
    input: 'src/index.js',
    output: {
      file: 'dist/index.cjs',
      format: 'cjs',
      sourcemap: true,
      exports: 'named',
    },
    external: ['@grpc/grpc-js', '@grpc/proto-loader', 'axios', 'form-data', 'google-protobuf', 'fs', 'path', 'http2'],
    plugins: [
      resolve({ preferBuiltins: true }),
      commonjs(),
      json(),
    ],
  },
];
