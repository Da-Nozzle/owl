from collections.abc import Mapping, Sequence

import dagster as dg

from owl_dagster_orchestration.snapshots import FetchPlan
from owl_ingest.ports.downloader import FetchedFile
from owl_ingest.ports.raw_archive import file_record

# Beyond this many files, a snapshot's metadata gives their count and size instead: a Places
# sweep's thousand-odd pages would store half a megabyte with each event.
LISTED_FILES_LIMIT = 100


def files_metadata(downloads: Mapping[str, FetchedFile]) -> dict[str, dict[str, object]]:
    return {name: file_record(download) for name, download in downloads.items()}


def files_listing(files: Mapping[str, Mapping[str, object]]) -> dict[str, object]:
    if len(files) <= LISTED_FILES_LIMIT:
        return dict(files)
    return {
        "file_count": len(files),
        "size": sum(int(str(record["size"])) for record in files.values()),
        "listed_in": "the snapshot's manifest, <archive>/<source>/_snapshots/<snapshot>.json",
    }


def plan_metadata(plan: FetchPlan) -> dict[str, dg.MetadataValue]:
    return {
        "paid_requests": dg.MetadataValue.int(plan.paid_requests),
        "max_paid_requests": dg.MetadataValue.int(plan.max_paid_requests),
        "summary": dg.MetadataValue.md(plan.summary),
        "parts": dg.MetadataValue.md(markdown_table(plan.parts)),
    }


def markdown_table(rows: Sequence[Mapping[str, object]]) -> str:
    if not rows:
        return "Nothing to plan."
    columns = list(rows[0])
    lines = [
        "| " + " | ".join(columns) + " |",
        "|" + "|".join("---" for _ in columns) + "|",
        *("| " + " | ".join(str(row.get(column, "")) for column in columns) + " |" for row in rows),
    ]
    return "\n".join(lines)
