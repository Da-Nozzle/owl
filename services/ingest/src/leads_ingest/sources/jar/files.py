from leads_ingest.ports.downloader import Download, Downloader
from leads_ingest.ports.raw_archive import RawArchive
from leads_ingest.sources.jar.settings import JarSettings
from leads_ingest.sources.spinta.exports import archive_exports

SOURCE_NAME = "jar"
# Its dbt models read the month from the archive path (_snapshot_month).
SNAPSHOT_KEY_FORMAT = "%Y-%m"
# Each dataset by the file name its dbt model reads. A seat (Buveine) refers to an entity by its
# `_id`, so both are fetched in the same snapshot.
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
    urls = {
        name: settings.download_url_template.format(dataset=dataset)
        for name, dataset in DATASETS.items()
    }
    return archive_exports(SOURCE_NAME, snapshot, urls, archive, downloader)
