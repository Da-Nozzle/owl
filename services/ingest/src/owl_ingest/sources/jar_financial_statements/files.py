from owl_ingest.ports.downloader import StreamedDownload, StreamingDownloader
from owl_ingest.ports.raw_archive import RawArchive
from owl_ingest.sources.jar_financial_statements.settings import JarFinancialStatementsSettings
from owl_ingest.sources.spinta.pages import archive_pages

SOURCE_NAME = "jar_financial_statements"
# Its dbt models read the month from the archive path (_snapshot_month).
SNAPSHOT_KEY_FORMAT = "%Y-%m"
# Each dataset by the name its files start with. The statements hold a row per line of every
# filing since 2015 (2.9 and 4.2 GB as CSV), so each is kept gzip-compressed, in pages
# (spinta.pages). They refer to the filing company by its JAR entity's `_id`.
DATASETS = {
    "PelnoAtaskaita": "pelno_ataskaitos/PelnoAtaskaita",
    "BalansoAtaskaita": "balanso_ataskaitos/BalansoAtaskaita",
    "Nepateikes": "fa_dokumentu_nepateike/Nepateikes",
    "VeluojantisPateikti": "fa_veluojantys/VeluojantisPateikti",
}


def archive_statement_files(
    snapshot: str,
    archive: RawArchive,
    settings: JarFinancialStatementsSettings,
    downloader: StreamingDownloader,
) -> dict[str, StreamedDownload]:
    """Saves each dataset to the snapshot in pages of `settings.page_rows` rows, by file name.
    Completing the snapshot is the caller's."""
    archived = {}
    for name, dataset in DATASETS.items():
        export_url = settings.download_url_template.format(dataset=dataset)
        archived |= archive_pages(
            SOURCE_NAME, snapshot, archive, name, export_url, settings.page_rows, downloader
        )
    return archived
