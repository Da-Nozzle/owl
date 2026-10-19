import json
from datetime import UTC, datetime

from owl_ingest.sources.spinta.changelog import Changelog
from owl_ingest.sources.vmi_tax_arrears.changes import archive_changes
from owl_testing.ingest import FakeSpinta, InMemoryRawArchive, change_entry

ARREARS = "https://vmi.test/NepriemokosSuma"
# The list's first load, the next mornings' changes, and one made after November's tick.
CHANGES = [
    change_entry(1, "2024-11-18T21:10:46", _op="insert"),
    change_entry(2, "2024-11-18T21:10:46", _op="insert"),
    change_entry(3, "2024-11-19T15:15:06", _op="upsert"),
    change_entry(5, "2024-11-20T06:05:34", _op="delete"),
    change_entry(6, "2024-11-21T06:03:16"),
]
# Snapshot 2024-11: from the tick of 21 October to that of 21 November, 06:00 in Vilnius.
MONTH_STARTS, MONTH_ENDS = "2024-10-21T03:00:00", "2024-11-21T04:00:00"


def utc(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def test_a_snapshot_archives_the_months_changes_a_page_each() -> None:
    # Arrange
    archive = InMemoryRawArchive()
    changelog = Changelog(ARREARS, FakeSpinta(ARREARS, CHANGES), page_size=2)

    # Act
    archived = archive_changes("2024-11", utc(MONTH_STARTS), utc(MONTH_ENDS), archive, changelog)

    # Assert
    pages = {
        name: [entry["_cid"] for entry in json.loads(page.body)["_data"]]
        for name, page in archived.items()
    }
    assert pages == {
        "changes-000000000001.json": [1, 2],
        "changes-000000000003.json": [3, 5],
    }
    assert set(archive.downloads) == {("vmi_tax_arrears", "2024-11", name) for name in pages}
    assert archive.snapshots("vmi_tax_arrears") == {}


def test_a_month_without_changes_archives_nothing() -> None:
    # Arrange
    archive = InMemoryRawArchive()

    # Act
    archived = archive_changes(
        "2024-10",
        utc("2024-09-21T03:00:00"),
        utc(MONTH_STARTS),
        archive,
        Changelog(ARREARS, FakeSpinta(ARREARS, CHANGES), page_size=2),
    )

    # Assert
    assert archived == {}
    assert archive.downloads == {}
