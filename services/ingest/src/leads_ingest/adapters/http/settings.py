from dataclasses import dataclass

from leads_config.configuration import Configuration


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """A request is sent up to `attempts` times. Before each retry it waits what the server's
    Retry-After asks for or, if it asks for nothing, `backoff_seconds`, doubled at each retry;
    at most `max_wait_seconds` either way."""

    attempts: int
    backoff_seconds: float
    max_wait_seconds: float

    def wait_seconds(self, attempt: int, retry_after_seconds: float | None = None) -> float:
        """The wait after failed attempt `attempt` (1 for the first)."""
        if retry_after_seconds is None:
            retry_after_seconds = self.backoff_seconds * 2 ** (attempt - 1)
        return min(retry_after_seconds, self.max_wait_seconds)


@dataclass(frozen=True, slots=True)
class HttpClientSettings:
    """How a source's requests are sent, from its `<source>.http` table: how long its server may
    stay silent, what is retried and how, and how far apart requests start."""

    # How long the server may send nothing (to connect, or between two reads) before the attempt
    # is given up.
    timeout_seconds: float
    retry: RetryPolicy
    # The least time between the starts of two requests, retries included; 0 for none.
    min_interval_seconds: float


def load_http_client_settings(config: Configuration, key: str) -> HttpClientSettings:
    """The table at `key` (`sodra.http`, ...), every setting required."""
    attempts = config.get_int(f"{key}.attempts")
    if attempts < 1:
        raise ValueError(f"'{key}.attempts' must be at least 1: {attempts}")
    timeout = seconds(config, f"{key}.timeout_seconds")
    if timeout == 0:
        raise ValueError(f"'{key}.timeout_seconds' must be more than 0")
    return HttpClientSettings(
        timeout_seconds=timeout,
        retry=RetryPolicy(
            attempts=attempts,
            backoff_seconds=seconds(config, f"{key}.backoff_seconds"),
            max_wait_seconds=seconds(config, f"{key}.max_wait_seconds"),
        ),
        min_interval_seconds=seconds(config, f"{key}.min_interval_seconds"),
    )


def seconds(config: Configuration, key: str) -> float:
    value = config.get_float(key)
    if value < 0:
        raise ValueError(f"{key!r} must not be negative: {value}")
    return value
