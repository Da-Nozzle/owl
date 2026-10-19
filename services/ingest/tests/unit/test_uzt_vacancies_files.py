import pytest

from owl_ingest.ports.downloader import StreamedDownload
from owl_ingest.sources.spinta.pages import IncompleteExportError
from owl_ingest.sources.uzt_vacancies.files import COLUMNS, archive_vacancies
from owl_ingest.sources.uzt_vacancies.settings import UztVacanciesSettings
from owl_testing.ingest import (
    FakeSpinta,
    FakeStreamingDownloader,
    InMemoryRawArchive,
    build_download,
    change_entry,
)

VACANCIES = "https://uzt.test/Vieta"
SETTINGS = UztVacanciesSettings(
    vacancies_url=VACANCIES,
    snapshot_schedule="0 6 21 * *",
    export_attempts=3,
    export_wait_seconds=300,
    page_rows=2,
    fetch_max_runtime_seconds=7_200,
)
CHANGES = [change_entry(7, "2026-10-17T02:38:00"), change_entry(9, "2026-10-18T02:40:18")]
EXPORT = f"{VACANCIES}/:format/csv"
SELECTED = f"{EXPORT}?select({','.join(COLUMNS)})&limit(2)"
COUNT, FIRST, SECOND = f"{EXPORT}?count()", SELECTED, f'{SELECTED}&page("Wv2")'
HEADER = b"_type,_id,_revision,_page.next,darbo_vietos_id\n"
# The token is on a response's last row only; a quoted field may span lines.
FIRST_PAGE = HEADER + b'uzt/ldv/Vieta,v1,r1,,DV-01-4\nuzt/ldv/Vieta,v2,r2,Wv2,"DV-01-\n6"\n'
LAST_PAGE = HEADER + b"uzt/ldv/Vieta,v3,r3,Wv3,DV-59-996710668\n"
PERSONAL = {"darbdavio_kontaktinis_asmuo", "darbdavio_tel_nr", "darbdavio_mob_nr"}


def counted(rows: int) -> bytes:
    return f"count()\n{rows}\n".encode()


def serving(**answers: list[tuple[str, bytes]]) -> FakeStreamingDownloader:
    """Each URL in `answers` answered once per (ETag, body), in that order."""
    return FakeStreamingDownloader(
        {
            url: [build_download(body, url=url, etag=etag) for etag, body in queued]
            for url, queued in answers.items()
        }
    )


def fetch(
    streaming: FakeStreamingDownloader,
    archive: InMemoryRawArchive | None = None,
    waits: list[float] | None = None,
) -> dict[str, StreamedDownload]:
    waits = waits if waits is not None else []
    return archive_vacancies(
        "2026-10",
        archive if archive is not None else InMemoryRawArchive(),
        SETTINGS,
        FakeSpinta(VACANCIES, CHANGES),
        streaming,
        wait=waits.append,
    )


def test_the_export_holding_the_newest_change_is_saved_in_pages() -> None:
    # Arrange
    streaming = serving(
        **{
            COUNT: [('"revision-9"', counted(3))],
            FIRST: [('"revision-9"', FIRST_PAGE)],
            SECOND: [('"revision-9"', LAST_PAGE)],
        }
    )
    archive = InMemoryRawArchive()

    # Act
    archived = fetch(streaming, archive)

    # Assert
    assert list(archived) == ["Vieta.csv.gz", "Vieta.2.csv.gz"]
    assert streaming.streamed == [COUNT, FIRST, SECOND]
    saved = {name: download.body for (_, _, name), download in archive.downloads.items()}
    assert saved == {"Vieta.csv.gz": FIRST_PAGE, "Vieta.2.csv.gz": LAST_PAGE}
    assert archive.snapshots("uzt_vacancies") == {}


def test_the_employers_contact_people_are_never_fetched() -> None:
    # Assert: each page selects these columns alone, the paging's _type first and _page.
    assert SELECTED.startswith(f"{EXPORT}?select(_type,_id,_revision,_page,darbo_vietos_id,")
    assert {"jar_kodas", "darbdavio_bustine"} <= set(COLUMNS)
    assert not PERSONAL & set(COLUMNS)
    assert not any("el_pastas" in column for column in COLUMNS)


def test_a_cached_count_from_before_the_newest_change_is_asked_again_before_any_page() -> None:
    # Arrange
    streaming = serving(
        **{
            COUNT: [('"revision-7"', counted(2)), ('"revision-9"', counted(2))],
            FIRST: [('"revision-9"', FIRST_PAGE)],
        }
    )
    waits: list[float] = []

    # Act
    archived = fetch(streaming, waits=waits)

    # Assert
    assert list(archived) == ["Vieta.csv.gz"]
    assert streaming.streamed == [COUNT, COUNT, FIRST]
    assert waits == [SETTINGS.export_wait_seconds]


def test_a_cached_page_from_before_the_newest_change_asks_for_the_export_again() -> None:
    # Arrange
    streaming = serving(
        **{
            COUNT: [('"revision-9"', counted(3))] * 2,
            FIRST: [('"revision-7"', FIRST_PAGE), ('"revision-9"', FIRST_PAGE)],
            SECOND: [('"revision-9"', LAST_PAGE)],
        }
    )
    waits: list[float] = []

    # Act
    archived = fetch(streaming, waits=waits)

    # Assert
    assert list(archived) == ["Vieta.csv.gz", "Vieta.2.csv.gz"]
    assert streaming.streamed == [COUNT, FIRST, COUNT, FIRST, SECOND]
    assert waits == [SETTINGS.export_wait_seconds]


def test_an_export_that_never_matches_the_changelog_is_not_archived() -> None:
    # Arrange
    archive = InMemoryRawArchive()
    stale = serving(**{COUNT: [('"revision-7"', counted(3))] * SETTINGS.export_attempts})

    # Act / Assert
    with pytest.raises(ValueError, match="revision-7"):
        fetch(stale, archive)
    assert archive.downloads == {}


def test_a_page_cut_off_fails_the_fetch_without_asking_again() -> None:
    # Arrange
    streaming = serving(
        **{
            COUNT: [('"revision-9"', counted(3))],
            FIRST: [('"revision-9"', HEADER + b"uzt/ldv/Vieta,v1,r1,,DV-01-4\n")],
        }
    )
    waits: list[float] = []

    # Act / Assert
    with pytest.raises(IncompleteExportError, match=r"Vieta\.csv\.gz was cut off"):
        fetch(streaming, waits=waits)
    assert waits == []


def test_an_error_page_sent_as_a_success_is_not_archived() -> None:
    # Arrange
    archive = InMemoryRawArchive()
    error_page = b"<html><body>500 Internal Server Error</body></html>\n"
    streaming = serving(
        **{COUNT: [('"revision-9"', counted(3))], FIRST: [('"revision-9"', error_page)]}
    )

    # Act / Assert
    with pytest.raises(ValueError, match=r"Vieta\.csv\.gz"):
        fetch(streaming, archive)
    assert archive.downloads == {}
