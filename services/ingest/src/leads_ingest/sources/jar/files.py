from leads_ingest.ports.downloader import Download, Downloader
from leads_ingest.ports.raw_archive import RawArchive
from leads_ingest.sources import spinta
from leads_ingest.sources.jar.settings import JarSettings

SOURCE_NAME = "jar"
# The register is fetched once a month, so a snapshot is keyed by its month, which its dbt
# models read from the archive path (_snapshot_month).
SNAPSHOT_KEY_FORMAT = "%Y-%m"
# The register's datasets a snapshot fetches, each by the file name its dbt model reads: the
# registered entities, the classifiers their legal form and status refer to by `_id`, and their
# seats (registered offices), which refer to an entity by its `_id` and so are resolved in the
# same snapshot, and to the seat's address in the Address Register (the address_register source).
DATASETS = {
    "JuridinisAsmuo.csv": "iregistruoti/JuridinisAsmuo",
    "Forma.csv": "formos_statusai/Forma",
    "Statusas.csv": "formos_statusai/Statusas",
    "Buveine.csv": "buveines/Buveine",
}


def archive_register_files(
    snapshot: str,
    archive: RawArchive,
    settings: JarSettings,
    downloader: Downloader,
) -> dict[str, Download]:
    """Fetches each of the register's datasets whole, as get.data.gov.lt exports it (one CSV),
    into the archive, where dbt reads them. Returns each file's download by name; completing the
    snapshot is the caller's."""
    urls = {
        name: settings.download_url_template.format(dataset=dataset)
        for name, dataset in DATASETS.items()
    }
    return spinta.archive_exports(SOURCE_NAME, snapshot, urls, archive, downloader)
