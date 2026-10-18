import json
from datetime import UTC, datetime

import pytest

from leads_ingest.sources.spinta.changelog import Changelog
from leads_ingest.sources.vmi.daily_changes import archive_changes
from leads_ingest.sources.vmi.monthly_register import (
    ACTIVITIES_FILE,
    LAST_CHANGE_FILE,
    REGISTER_FILE,
    archive_register,
)
from leads_ingest.sources.vmi.settings import VmiSettings
from leads_testing.ingest import FakeSpinta, InMemoryRawArchive, build_download, change_entry

REGISTER = "https://vmi.test/MokesciuMoketojas"
ACTIVITIES = "https://vmi.test/EkonominesVeiklosRusis"
SETTINGS = VmiSettings(
    register_url=REGISTER,
    activities_url=ACTIVITIES,
    daily_changes_schedule="0 6 * * *",
    first_daily_changes="2026-09-01",
    monthly_register_schedule="0 6 21 * *",
    changelog_page_size=10_000,
    register_export_attempts=5,
    register_export_wait_seconds=65,
)
CHANGES = [
    change_entry(10, "2026-09-16T23:57:00"),
    change_entry(12, "2026-09-17T23:57:18"),
    change_entry(15, "2026-09-17T23:57:46"),
    change_entry(16, "2026-09-19T00:03:50"),
]
REGISTER_CSV = b"_type,_id,ja_kodas\nMokesciuMoketojas,r1,110607882\n"
ACTIVITIES_CSV = b"_type,_id,kodas\nEkonominesVeiklosRusis,e1,561000\n"


def utc(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def test_a_snapshot_archives_the_changes_made_in_its_window() -> None:
    # Arrange
    spinta = FakeSpinta(REGISTER, CHANGES)
    archive = InMemoryRawArchive()

    # Act
    archived = archive_changes(
        "2026-09-18",
        utc("2026-09-17T03:00:00"),
        utc("2026-09-18T03:00:00"),
        archive,
        Changelog(REGISTER, spinta, SETTINGS.changelog_page_size),
    )

    # Assert
    [(name, page)] = archived.items()
    assert name == "changes-000000000011.json"
    assert [entry["_cid"] for entry in json.loads(page.body)["_data"]] == [12, 15]
    assert set(archive.downloads) == {("vmi_daily_changes", "2026-09-18", name)}
    assert archive.snapshots("vmi_daily_changes") == {}


def test_a_day_without_changes_archives_nothing() -> None:
    # Arrange
    archive = InMemoryRawArchive()

    # Act
    archived = archive_changes(
        "2026-09-19",
        utc("2026-09-18T03:00:00"),
        utc("2026-09-19T00:00:00"),
        archive,
        Changelog(REGISTER, FakeSpinta(REGISTER, CHANGES), SETTINGS.changelog_page_size),
    )

    # Assert
    assert archived == {}
    assert archive.downloads == {}


def spinta_exporting(*etags: str) -> FakeSpinta:
    """The register as the portal serves it: its export once per ETag, in that order."""
    export_url = f"{REGISTER}/:format/csv"
    return FakeSpinta(
        REGISTER,
        CHANGES,
        responses={
            export_url: [build_download(REGISTER_CSV, url=export_url, etag=etag) for etag in etags],
            f"{ACTIVITIES}/:format/csv": [build_download(ACTIVITIES_CSV)],
        },
    )


def test_an_export_is_archived_with_the_changelogs_newest_change_it_matches() -> None:
    # Arrange
    archive = InMemoryRawArchive()

    # Act
    archived = archive_register(
        "2026-09", archive, SETTINGS, spinta_exporting('"revision-16"'), wait=lambda _: None
    )

    # Assert
    assert list(archived) == [REGISTER_FILE, ACTIVITIES_FILE, LAST_CHANGE_FILE]
    assert json.loads(archived[LAST_CHANGE_FILE].body)["_data"][0]["_cid"] == 16
    assert set(archive.downloads) == {
        ("vmi_monthly_register", "2026-09", name) for name in archived
    }


def test_an_export_from_before_the_newest_change_is_fetched_again() -> None:
    # Arrange
    waits: list[float] = []

    # Act
    archived = archive_register(
        "2026-09",
        InMemoryRawArchive(),
        SETTINGS,
        spinta_exporting("revision-15", "revision-16"),
        wait=waits.append,
    )

    # Assert
    assert archived[REGISTER_FILE].etag == "revision-16"
    assert waits == [SETTINGS.register_export_wait_seconds]


def test_an_export_that_never_matches_the_changelog_is_not_archived() -> None:
    # Arrange
    archive = InMemoryRawArchive()

    # Act / Assert
    with pytest.raises(ValueError, match="revision-15"):
        archive_register(
            "2026-09",
            archive,
            SETTINGS,
            spinta_exporting(*["revision-15"] * SETTINGS.register_export_attempts),
            wait=lambda _: None,
        )
    assert archive.downloads == {}
