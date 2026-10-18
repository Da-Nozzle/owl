import pytest

from leads_http.settings import (
    HttpClientSettings,
    RetryPolicy,
    load_http_client_settings,
)
from leads_testing.config import build_config


def test_a_sources_client_is_configured_by_its_http_table() -> None:
    # Arrange
    config = build_config(
        {
            "vmi.daily_changes_http.timeout_seconds": 30,
            "vmi.daily_changes_http.attempts": 4,
            "vmi.daily_changes_http.backoff_seconds": 0.5,
            "vmi.daily_changes_http.max_wait_seconds": 120,
            "vmi.daily_changes_http.min_interval_seconds": 1,
        }
    )

    # Act
    settings = load_http_client_settings(config, "vmi.daily_changes_http")

    # Assert
    assert settings == HttpClientSettings(
        timeout_seconds=30,
        retry=RetryPolicy(attempts=4, backoff_seconds=0.5, max_wait_seconds=120),
        min_interval_seconds=1,
    )


@pytest.mark.parametrize(
    ("setting", "value", "message"),
    [
        ("attempts", 0, "must be at least 1"),
        ("timeout_seconds", 0, "must be more than 0"),
        ("backoff_seconds", -1, "must not be negative"),
        ("min_interval_seconds", -0.5, "must not be negative"),
    ],
)
def test_a_setting_out_of_range_is_rejected(setting: str, value: float, message: str) -> None:
    # Arrange
    config = build_config({f"sodra.http.{setting}": value})

    # Act / Assert
    with pytest.raises(ValueError, match=rf"'sodra\.http\.{setting}' {message}"):
        load_http_client_settings(config, "sodra.http")


def test_a_retry_waits_what_the_server_asks_or_a_doubling_backoff_up_to_the_most() -> None:
    # Arrange
    policy = RetryPolicy(attempts=10, backoff_seconds=2, max_wait_seconds=60)

    # Act
    waits = [policy.wait_seconds(attempt) for attempt in range(1, policy.attempts)]

    # Assert
    assert waits == [2, 4, 8, 16, 32, 60, 60, 60, 60]
    assert (policy.wait_seconds(1, 7), policy.wait_seconds(1, 600)) == (7, 60)
