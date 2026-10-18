import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from leads_ingest.ports.downloader import Download
from leads_ingest.ports.raw_archive import SnapshotManifest, file_record

FETCHED_AT = datetime(2026, 9, 29, 11, 23, tzinfo=UTC)


def build_download(
    body: bytes = b"PK",
    *,
    url: str = "https://source.test/file.zip",
    fetched_at: datetime = FETCHED_AT,
    last_modified: str | None = None,
    etag: str | None = None,
    request: Mapping[str, object] | None = None,
) -> Download:
    return Download(
        url=url,
        body=body,
        fetched_at=fetched_at,
        last_modified=last_modified,
        etag=etag,
        request=request,
    )


@dataclass
class FakeDownloader:
    """A Downloader that answers every URL with the same body and records the URLs asked for."""

    body: bytes
    requested: list[str] = field(default_factory=list)

    def download(self, url: str) -> Download:
        self.requested.append(url)
        return build_download(self.body, url=url)


@dataclass
class FakeSpinta:
    """A Downloader answering as get.data.gov.lt does for one model at `model_url`: its changelog
    from `changes` (entries in id order, ids may have gaps), read by id with `limit(n)` and
    `select(...)`, `-1` for the newest; and any other URL with the next of the downloads queued
    for it in `responses`. Records the URLs asked for."""

    model_url: str
    changes: list[dict[str, object]] = field(default_factory=list)
    responses: dict[str, list[Download]] = field(default_factory=dict)
    requested: list[str] = field(default_factory=list)

    def download(self, url: str) -> Download:
        self.requested.append(url)
        if url in self.responses:
            return self.responses[url].pop(0)
        path, _, query = url.partition("?")
        position = path.removeprefix(f"{self.model_url}/:changes/")
        if position == "-1":
            entries = self.changes[-1:]
        else:
            entries = [entry for entry in self.changes if int(str(entry["_cid"])) >= int(position)]
            limit = re.search(r"limit\((\d+)\)", query)
            entries = entries[: int(limit.group(1))] if limit else entries
        if "select(" in query:
            entries = [
                {key: entry[key] for key in ("_cid", "_created", "_revision")} for entry in entries
            ]
        return build_download(json.dumps({"_data": entries}).encode(), url=url)


def change_entry(change_id: int, made_at: str, **values: object) -> dict[str, object]:
    """A changelog entry as Spinta sends it: a patch of `values` unless `_op` is given."""
    return {
        "_cid": change_id,
        "_created": made_at,
        "_op": "patch",
        "_id": f"row-{change_id}",
        "_txn": "txn",
        "_revision": f"revision-{change_id}",
        **values,
    }


@dataclass
class InMemoryRawArchive:
    """A RawArchive in dicts: downloads keyed (source, snapshot, file name), and the manifests of
    complete snapshots keyed (source, snapshot), each with how often it was completed as its
    version."""

    downloads: dict[tuple[str, str, str], Download] = field(default_factory=dict)
    manifests: dict[tuple[str, str], tuple[SnapshotManifest, int]] = field(default_factory=dict)

    def save(self, source: str, snapshot: str, name: str, download: Download) -> None:
        self.downloads[(source, snapshot, name)] = download

    def complete(
        self,
        source: str,
        snapshot: str,
        downloads: Mapping[str, Download],
        completed_at: datetime,
    ) -> None:
        files = {name: file_record(download) for name, download in downloads.items()}
        _, version = self.manifests.get((source, snapshot), (None, 0))
        self.manifests[(source, snapshot)] = (SnapshotManifest(completed_at, files), version + 1)

    def snapshots(self, source: str) -> dict[str, str]:
        return {
            snapshot: str(version)
            for (saved, snapshot), (_, version) in self.manifests.items()
            if saved == source
        }

    def manifest(self, source: str, snapshot: str) -> SnapshotManifest:
        return self.manifests[(source, snapshot)][0]

    def version_id(self, source: str, snapshot: str, name: str) -> str | None:
        return None


@dataclass
class FakePlacesApi:
    """A PlacesApi answering Text Search as Google does over `places` (each query's places,
    each a dict with at least `id` and `location`): the query's places inside the request's
    rectangle, edges included, in order, at most `cap`, in pages of the request's `pageSize`
    with a `nextPageToken` while more remain, `{}` when there are none. A place is sent with
    the fields the field mask names. Records every request with its field mask."""

    places: dict[str, list[dict[str, Any]]]
    cap: int = 60
    requests: list[tuple[dict[str, Any], str]] = field(default_factory=list)

    def search_text(self, request: Mapping[str, Any], field_mask: str) -> Download:
        self.requests.append((dict(request), field_mask))
        rectangle = request["locationRestriction"]["rectangle"]
        low, high = rectangle["low"], rectangle["high"]
        inside = [
            place
            for place in self.places.get(request["textQuery"], [])
            if low["latitude"] <= place["location"]["latitude"] <= high["latitude"]
            and low["longitude"] <= place["location"]["longitude"] <= high["longitude"]
        ][: self.cap]
        start, size = int(str(request.get("pageToken", 0))), int(str(request["pageSize"]))
        fields = {name.removeprefix("places.") for name in field_mask.split(",")}
        document: dict[str, object] = {}
        if page := inside[start : start + size]:
            document["places"] = [{k: v for k, v in place.items() if k in fields} for place in page]
        if start + size < len(inside):
            document["nextPageToken"] = str(start + size)
        return build_download(
            json.dumps(document).encode(),
            url="https://places.test/v1/places:searchText",
            request={"field_mask": field_mask, "body": dict(request)},
        )


def place(place_id: str, latitude: float, longitude: float, **values: object) -> dict[str, Any]:
    """A place as the Places API sends it with every field asked for: `values` add to its id,
    location and name."""
    return {
        "id": place_id,
        "location": {"latitude": latitude, "longitude": longitude},
        "displayName": {"text": f"Place {place_id}", "languageCode": "lt"},
        **values,
    }


@dataclass
class FakeLakeReader:
    """A LakeReader answering every query with the same rows, and recording the queries."""

    answer: list[dict[str, object]]
    queries: list[str] = field(default_factory=list)

    def rows(self, sql: str) -> list[Mapping[str, object]]:
        self.queries.append(sql)
        return list(self.answer)
