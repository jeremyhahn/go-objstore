/**
 * QUIC Integration — explicit skip.
 *
 * Node.js has no native HTTP/3 (QUIC) transport. The TypeScript QuicClient
 * uses the Fetch API over HTTP/1.1 or HTTP/2, which targets the REST endpoint.
 * Running "QUIC" integration tests this way would silently re-run the REST
 * test suite under a misleading QUIC label and give false protocol parity.
 *
 * Per the canonical SDK test contract:
 *   QUIC: real where the language supports HTTP/3; explicit LOGGED skip for
 *   TypeScript/Node — never silently faked.
 *
 * What IS tested:
 *   - REST integration tests cover the server end-to-end for all 19 ops.
 *   - gRPC integration tests cover a second real transport.
 *   - Unit tests (tests/unit/quic-client.test.ts) fully exercise all QuicClient
 *     code paths via mocks, achieving >93 % line coverage on the client itself.
 *
 * When QUIC integration becomes available (e.g. via a Node.js native HTTP/3
 * library or QUIC-aware proxy), remove the skip and wire in a real QUIC client
 * pointed at the QUIC port.
 */

describe('QUIC Integration', () => {
  it.skip(
    'QUIC integration is explicitly skipped — ' +
      'TypeScript/Node has no native HTTP/3 transport, so QuicClient speaks ' +
      'HTTP/1.1 over TCP and cannot reach the bundled QUIC server, which is ' +
      'UDP/HTTP3-only. ' +
      'Coverage provided by REST + gRPC integration suites and QUIC unit tests.',
    () => {}
  );
});
