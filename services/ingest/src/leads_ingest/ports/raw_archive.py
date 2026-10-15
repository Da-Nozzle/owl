from typing import Protocol

from leads_ingest.months import Month
from leads_ingest.ports.downloader import Download


class RawArchive(Protocol):
    """Raw downloads per source and run month. Live runs save to it before loading; replay runs
    load from a copy of it and download nothing."""

    def save(self, source: str, month: Month, name: str, download: Download) -> None: ...

    def load(self, source: str, month: Month, name: str) -> Download: ...

    def exists(self, source: str, month: Month, name: str) -> bool: ...

    def months(self, source: str) -> list[Month]: ...

    def version_id(self, source: str, month: Month, name: str) -> str | None:
        """The storage version of the file as it is now, for the audit trail; None where the
        storage keeps no versions (a local directory)."""
        ...
