from leads_ingest.ports.downloader import Download, Downloader
from leads_ingest.ports.raw_archive import RawArchive
from leads_ingest.sources import spinta
from leads_ingest.sources.address_register.settings import AddressRegisterSettings

SOURCE_NAME = "address_register"
# The register is fetched once a month, so a snapshot is keyed by its month, which its dbt
# models read from the archive path (_snapshot_month).
SNAPSHOT_KEY_FORMAT = "%Y-%m"
# The register's datasets a snapshot fetches, each by the file name its dbt model reads: the
# addresses of buildings and land plots, the addresses of premises (each in its building), and
# the streets, settlements and municipalities a building's address refers to by `_id`. A JAR seat
# refers to either kind of address by the `_id` both name `aob_kodas._id`, so the register's own
# address dataset (Adresas, 317 MB) is not needed to resolve it.
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
    """Fetches each of the register's datasets whole, as get.data.gov.lt exports it (one CSV),
    into the archive, where dbt reads them. Returns each file's download by name; completing the
    snapshot is the caller's."""
    urls = {
        name: settings.download_url_template.format(dataset=dataset)
        for name, dataset in DATASETS.items()
    }
    return spinta.archive_exports(SOURCE_NAME, snapshot, urls, archive, downloader)
