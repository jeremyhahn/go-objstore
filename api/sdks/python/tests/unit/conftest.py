"""Shared pytest fixtures for unit tests."""

import pytest


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make tenacity retries instantaneous.

    The REST client decorates every operation with an exponential-backoff
    retry. Patching tenacity's sleep keeps the retry behaviour exercised while
    removing the real wall-clock delay from the test suite.
    """
    monkeypatch.setattr("tenacity.nap.time.sleep", lambda _seconds: None)
