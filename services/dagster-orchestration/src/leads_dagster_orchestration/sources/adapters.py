from collections.abc import Callable
from dataclasses import dataclass

from leads_config.configuration import Configuration
from leads_http.client import build_http_client
from leads_http.settings import load_http_client_settings
from leads_ingest.adapters.archive.fsspec_archive import FsspecRawArchive, open_fsspec_archive
from leads_ingest.adapters.ducklake.search_areas import DuckLakeSearchAreaReader
from leads_ingest.adapters.http.downloader import HttpDownloader
from leads_ingest.adapters.http.places_api import HttpPlacesApi
from leads_ingest.adapters.s3.configuration import s3fs_options
from leads_ingest.ports.downloader import Downloader
from leads_ingest.ports.places_api import PlacesApi
from leads_ingest.ports.search_areas import SearchAreaReader
from leads_ingest.settings import PipelineMode, PipelineSettings, raw_archive_path
from leads_lake.settings import LakeSettings, LakeStorage


@dataclass(frozen=True, slots=True)
class LiveAdapters:
    """What only the live lake's fetches reach."""

    # A source's downloader, by its HTTP client's table (`sodra.http`, ...).
    downloader: Callable[[str], Downloader]
    places_api: Callable[[], PlacesApi]
    search_areas: SearchAreaReader


def build_live_adapters(config: Configuration, user_agent: str, lake: LakeSettings) -> LiveAdapters:
    def downloader(key: str) -> Downloader:
        return HttpDownloader(build_http_client(user_agent, load_http_client_settings(config, key)))

    places_client = build_http_client(
        user_agent, load_http_client_settings(config, "google_places.http")
    )

    def places_api() -> PlacesApi:
        # The key is read when a fetch first needs it: loading the definitions reads no secret.
        return HttpPlacesApi(
            client=places_client,
            api_key=config.get_str("google_places.api_key"),
            search_text_url=config.get_str("google_places.search_text_url"),
        )

    return LiveAdapters(
        downloader=downloader,
        places_api=places_api,
        search_areas=DuckLakeSearchAreaReader(
            lake, config.get_str("transform.duckdb_memory_limit")
        ),
    )


def build_raw_archive(
    mode: PipelineMode, settings: PipelineSettings, storage: LakeStorage
) -> FsspecRawArchive:
    path = raw_archive_path(mode, settings, storage)
    if mode is PipelineMode.LIVE:
        return open_fsspec_archive(path, s3fs_options(storage))
    return open_fsspec_archive(path)
