"""Legacy integration test module — superseded by test_comprehensive.py.

All tests previously in this file have been consolidated into
tests/integration/test_comprehensive.py, which is data-driven across REST,
gRPC, and QUIC and matches the canonical SDK test contract.

This file is intentionally empty to preserve the module's import path for any
external tooling that references it by name.
"""
