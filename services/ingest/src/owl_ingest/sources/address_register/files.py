from owl_ingest.ports.downloader import Download, Downloader
from owl_ingest.ports.raw_archive import RawArchive
from owl_ingest.sources.address_register.settings import AddressRegisterSettings
from owl_ingest.sources.spinta.exports import archive_exports

SOURCE_NAME = "address_register"
# Its dbt models read the month from the archive path (_snapshot_month).
SNAPSHOT_KEY_FORMAT = "%Y-%m"
# Each dataset by the file name its dbt model reads. A JAR seat refers to a building's or a
# premises' address by its `_id`, so the register's own 317 MB address dataset is not needed.
DATASETS = {
    "Pastatas.csv": "pastatas/Pastatas",
    "Patalpa.csv": "patalpa/Patalpa",
    "Gatve.csv": "gatve/Gatve",
    "GyvenamojiVietove.csv": "gyvenamojivietove/GyvenamojiVietove",
    "Savivaldybe.csv": "savivaldybe/Savivaldybe",
}


def archive_register_files(
    snapshot: str,
    archive: RawArchive,
    settings: AddressRegisterSettings,
    downloader: Downloader,
) -> dict[str, Download]:
    urls = {
        name: settings.download_url_template.format(dataset=dataset)
        for name, dataset in DATASETS.items()
    }
    return archive_exports(SOURCE_NAME, snapshot, urls, archive, downloader)
