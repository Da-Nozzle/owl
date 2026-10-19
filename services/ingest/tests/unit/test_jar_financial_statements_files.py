from owl_ingest.sources.jar_financial_statements.files import DATASETS, archive_statement_files
from owl_ingest.sources.jar_financial_statements.settings import JarFinancialStatementsSettings
from owl_testing.ingest import FakeStreamingDownloader, InMemoryRawArchive, build_download

URL_TEMPLATE = "https://jar.test/{dataset}/:format/csv"
SETTINGS = JarFinancialStatementsSettings(
    download_url_template=URL_TEMPLATE,
    snapshot_schedule="0 6 21 3,6,9,12 *",
    page_rows=2,
    fetch_max_runtime_seconds=14_400,
)
SOURCE = "jar_financial_statements"
VERSION = "d242757b"
INCOME = URL_TEMPLATE.format(dataset=DATASETS["PelnoAtaskaita"])
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
    """Every dataset counted as one row and sent as one page, unless `answers` answers its URL:
    each answer in order, of version VERSION unless given with its own."""
    responses: dict[str, list[bytes | tuple[bytes, str]]] = {}
    for dataset in DATASETS.values():
        url = URL_TEMPLATE.format(dataset=dataset)
        responses[f"{url}?count()"] = [counted(1)]
        responses[f"{url}?limit(2)"] = [HEADER + row("b1", "Wb1")]
    responses |= answers
    return FakeStreamingDownloader(
        {
            url: [
                build_download(body, url=url, etag=f'"{version}"')
                for body, version in (
                    answer if isinstance(answer, tuple) else (answer, VERSION) for answer in queued
                )
            ]
            for url, queued in responses.items()
        }
    )


def test_a_fetch_saves_each_dataset_in_pages_and_leaves_completing_the_snapshot() -> None:
    # Arrange
    downloader = serving(**{COUNT: [counted(3)], FIRST: [FIRST_PAGE], SECOND: [LAST_PAGE]})
    archive = InMemoryRawArchive()

    # Act
    archived = archive_statement_files("2026-09", archive, SETTINGS, downloader)

    # Assert
    assert list(archived) == [
        "PelnoAtaskaita.csv.gz",
        "PelnoAtaskaita.2.csv.gz",
        "BalansoAtaskaita.csv.gz",
        "Nepateikes.csv.gz",
        "VeluojantisPateikti.csv.gz",
    ]
    assert downloader.streamed[:4] == [
        COUNT,
        FIRST,
        SECOND,
        URL_TEMPLATE.format(dataset="balanso_ataskaitos/BalansoAtaskaita") + "?count()",
    ]
    saved = {name: download.body for (_, _, name), download in archive.downloads.items()}
    assert saved["PelnoAtaskaita.csv.gz"] == FIRST_PAGE
    assert saved["PelnoAtaskaita.2.csv.gz"] == LAST_PAGE
    assert saved["Nepateikes.csv.gz"] == HEADER + row("b1", "Wb1")
    assert archive.snapshots(SOURCE) == {}
