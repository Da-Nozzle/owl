from dataclasses import dataclass, field

from leads_ingest.adapters.http.downloader import ThrottledDownloader
from leads_testing.ingest import FakeDownloader


@dataclass
class FakeClock:
    """A monotonic clock that a wait moves forward."""

    now: float = 100.0
    waits: list[float] = field(default_factory=list)

    def __call__(self) -> float:
        return self.now

    def wait(self, seconds: float) -> None:
        self.waits.append(seconds)
        self.now += seconds


def test_requests_start_at_least_the_interval_apart() -> None:
    # Arrange
    clock = FakeClock()
    downloader = ThrottledDownloader(FakeDownloader(b"{}"), 1.0, clock=clock, wait=clock.wait)

    # Act
    downloader.download("https://spinta.test/a")
    clock.now += 0.25
    downloader.download("https://spinta.test/b")
    downloader.download("https://spinta.test/c")

    # Assert
    assert clock.waits == [0.75, 1.0]


def test_a_request_after_a_long_pause_does_not_wait() -> None:
    # Arrange
    clock = FakeClock()
    inner = FakeDownloader(b"{}")
    downloader = ThrottledDownloader(inner, 1.0, clock=clock, wait=clock.wait)

    # Act
    downloader.download("https://spinta.test/a")
    clock.now += 5
    downloader.download("https://spinta.test/b")

    # Assert
    assert clock.waits == []
    assert inner.requested == ["https://spinta.test/a", "https://spinta.test/b"]
