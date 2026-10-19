import json
from datetime import UTC, datetime

from owl_ingest.sources.spinta.changelog import Changelog
from owl_ingest.sources.vmi_taxes_paid.changes import archive_changes
from owl_testing.ingest import FakeSpinta, InMemoryRawArchive, change_entry

MODEL = "https://vmi.test/datasets/gov/vmi/ja_mokesciai/Moketojas"
PAGE_SIZE = 2
# Two monthly loads: each inserts the current year's rows and deletes the previous load's.
CHANGES = [
    change_entry(10, "2026-08-13T06:33:20", _op="insert", metai=2026, menuo=7, suma=302.86),
    change_entry(11, "2026-08-13T06:33:21", _op="insert", metai=2026, menuo=7, suma=17.0),
    change_entry(20, "2026-09-18T05:27:39", _op="delete"),
    change_entry(21, "2026-09-18T05:27:39", _op="insert", metai=2026, menuo=8, suma=2043.48),
    change_entry(22, "2026-09-18T05:27:40", _op="delete"),
    change_entry(23, "2026-09-18T05:27:41", _op="insert", metai=2026, menuo=8, suma=17.0),
]


def utc(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def test_a_snapshot_archives_the_loads_made_in_its_month_page_by_page() -> None:
    # Arrange
    archive = InMemoryRawArchive()

    # Act
    archived = archive_changes(
        "2026-09",
        utc("2026-08-21T03:00:00"),
        utc("2026-09-21T03:00:00"),
        archive,
        Changelog(MODEL, FakeSpinta(MODEL, CHANGES), PAGE_SIZE),
    )

    # Assert
    assert {
        name: [entry["_cid"] for entry in json.loads(page.body)["_data"]]
        for name, page in archived.items()
    } == {"changes-000000000012.json": [20, 21], "changes-000000000022.json": [22, 23]}
    assert set(archive.downloads) == {("vmi_taxes_paid", "2026-09", name) for name in archived}
    assert archive.snapshots("vmi_taxes_paid") == {}


def test_a_month_without_a_load_archives_nothing() -> None:
    # Arrange
    archive = InMemoryRawArchive()

    # Act
    archived = archive_changes(
        "2026-10",
        utc("2026-09-21T03:00:00"),
        utc("2026-10-21T03:00:00"),
        archive,
        Changelog(MODEL, FakeSpinta(MODEL, CHANGES), PAGE_SIZE),
    )

    # Assert
    assert archived == {}
    assert archive.downloads == {}
