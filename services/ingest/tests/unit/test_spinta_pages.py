import pytest

from owl_ingest.ports.downloader import StreamedDownload
from owl_ingest.sources.spinta.pages import (
    ChangedExportError,
    IncompleteExportError,
    archive_pages,
)
from owl_testing.ingest import FakeStreamingDownloader, InMemoryRawArchive, build_download

SOURCE = "jar_financial_statements"
VERSION = "d242757b"
INCOME = "https://jar.test/pelno_ataskaitos/PelnoAtaskaita/:format/csv"
COUNT, FIRST, SECOND = f"{INCOME}?count()", f"{INCOME}?limit(2)", f'{INCOME}?limit(2)&page("Wa2")'
HEADER = b"_type,_id,_revision,_base,_page.next,juridinis_asmuo._id,standard_name,reiksme\n"


def row(row_id: str, token: str = "", name: str = "PELNO ATASKAITA") -> bytes:
    return f"rc/jar/PelnoAtaskaita,{row_id},r{row_id},,{token},e1,{name},58708\n".encode()


# Spinta's next page token on a response's last row only; a form's name may span lines.
FIRST_PAGE = HEADER + row("a1") + row("a2", "Wa2", name='"BALANSAS\n(duomenys)"')
LAST_PAGE = HEADER + row("a3", "Wa3")


def counted(rows: int) -> bytes:
    return f"count()\n{rows}\n".encode()


def serving(**answers: list[bytes | tuple[bytes, str]]) -> FakeStreamingDownloader:
    """Each URL in `answers` answered in order, of version VERSION unless given with its own."""
    return FakeStreamingDownloader(
        {
            url: [
                build_download(body, url=url, etag=f'"{version}"')
                for body, version in (
                    answer if isinstance(answer, tuple) else (answer, VERSION) for answer in queued
                )
            ]
            for url, queued in answers.items()
        }
    )


def archive_income_statements(
    downloader: FakeStreamingDownloader, archive: InMemoryRawArchive | None = None
) -> dict[str, StreamedDownload]:
    archive = archive if archive is not None else InMemoryRawArchive()
    return archive_pages(SOURCE, "2026-09", archive, "PelnoAtaskaita", INCOME, 2, downloader)


def test_an_export_is_saved_in_pages_and_completing_the_snapshot_is_left() -> None:
    # Arrange
    downloader = serving(**{COUNT: [counted(3)], FIRST: [FIRST_PAGE], SECOND: [LAST_PAGE]})
    archive = InMemoryRawArchive()

    # Act
    archived = archive_income_statements(downloader, archive)

    # Assert
    assert list(archived) == ["PelnoAtaskaita.csv.gz", "PelnoAtaskaita.2.csv.gz"]
    assert downloader.streamed == [COUNT, FIRST, SECOND]
    saved = {name: download.body for (_, _, name), download in archive.downloads.items()}
    assert saved == {"PelnoAtaskaita.csv.gz": FIRST_PAGE, "PelnoAtaskaita.2.csv.gz": LAST_PAGE}
    assert archive.snapshots(SOURCE) == {}


def test_an_export_of_whole_pages_asks_for_no_empty_page_after_them() -> None:
    # Arrange
    downloader = serving(**{COUNT: [counted(2)], FIRST: [FIRST_PAGE]})

    # Act
    archived = archive_income_statements(downloader)

    # Assert
    assert list(archived) == ["PelnoAtaskaita.csv.gz"]
    assert SECOND not in downloader.streamed


@pytest.mark.parametrize(
    "page",
    [
        # As the cache sent the whole export: a response the portal dropped, kept as if whole.
        pytest.param(HEADER + row("a1"), id="ends on a whole row"),
        pytest.param(FIRST_PAGE[:-12], id="ends inside a row"),
        pytest.param(HEADER + row("a1") + row("a2"), id="its last row has no token"),
    ],
)
def test_a_page_cut_off_fails_the_fetch(page: bytes) -> None:
    # Arrange
    downloader = serving(**{COUNT: [counted(3)], FIRST: [page]})

    # Act / Assert
    with pytest.raises(IncompleteExportError, match=r"PelnoAtaskaita\.csv\.gz was cut off"):
        archive_income_statements(downloader)


def test_a_page_of_another_version_than_was_counted_fails_the_fetch() -> None:
    # Arrange
    downloader = serving(
        **{COUNT: [counted(3)], FIRST: [FIRST_PAGE], SECOND: [(LAST_PAGE, "f21794ae")]}
    )

    # Act / Assert
    with pytest.raises(ChangedExportError, match=r"PelnoAtaskaita\.2\.csv\.gz is version"):
        archive_income_statements(downloader)


def test_an_error_page_sent_as_a_success_is_not_archived() -> None:
    # Arrange
    error_page = b"<html><body>500 Internal Server Error</body></html>\n"
    downloader = serving(**{COUNT: [counted(3)], FIRST: [error_page]})
    archive = InMemoryRawArchive()

    # Act / Assert
    with pytest.raises(ValueError, match=r"PelnoAtaskaita\.csv\.gz is not a CSV export"):
        archive_income_statements(downloader, archive)
    assert archive.downloads == {}


def test_a_count_that_is_not_one_fails_the_fetch() -> None:
    # Arrange
    downloader = serving(**{COUNT: [b"<html><body>502 Bad Gateway</body></html>\n"]})

    # Act / Assert
    with pytest.raises(ValueError, match=r"PelnoAtaskaita/:format/csv\?count\(\) is not a count"):
        archive_income_statements(downloader)


def test_a_selection_is_asked_for_on_each_page_but_not_the_count() -> None:
    # Arrange
    selected = f"{INCOME}?select(_type,_id,_page,reiksme)&limit(2)"
    page = b"_type,_id,_page.next,reiksme\nrc/jar/PelnoAtaskaita,a1,Wa1,58708\n"
    downloader = serving(**{COUNT: [counted(1)], selected: [page]})

    # Act
    archived = archive_pages(
        SOURCE,
        "2026-09",
        InMemoryRawArchive(),
        "PelnoAtaskaita",
        INCOME,
        2,
        downloader,
        columns=("_type", "_id", "_page", "reiksme"),
    )

    # Assert
    assert list(archived) == ["PelnoAtaskaita.csv.gz"]
    assert downloader.streamed == [COUNT, selected]


@pytest.mark.parametrize(
    "columns",
    [("_id", "_type", "_page"), ("_type", "_id", "_page.next")],
    ids=["_type not first", "no _page"],
)
def test_a_selection_the_paging_cannot_read_is_refused(columns: tuple[str, ...]) -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="paged selection"):
        archive_pages(
            SOURCE,
            "2026-09",
            InMemoryRawArchive(),
            "PelnoAtaskaita",
            INCOME,
            2,
            serving(),
            columns=columns,
        )


def test_a_count_of_another_version_than_asked_for_fails_before_any_page() -> None:
    # Arrange
    downloader = serving(**{COUNT: [counted(3)]})

    # Act / Assert
    with pytest.raises(ChangedExportError, match=f"counted at version {VERSION}, not f21794ae"):
        archive_pages(
            SOURCE,
            "2026-09",
            InMemoryRawArchive(),
            "PelnoAtaskaita",
            INCOME,
            2,
            downloader,
            version="f21794ae",
        )
    assert downloader.streamed == [COUNT]
