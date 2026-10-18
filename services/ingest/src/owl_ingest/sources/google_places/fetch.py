import json

from owl_ingest.parallel import in_parallel
from owl_ingest.ports.downloader import Download
from owl_ingest.ports.places_api import PlacesApi
from owl_ingest.ports.raw_archive import RawArchive
from owl_ingest.sources.google_places.file_names import page_name
from owl_ingest.sources.google_places.plan import PlannedSearch, SweepPlan
from owl_ingest.sources.google_places.settings import GooglePlacesSettings
from owl_ingest.sources.google_places.text_search import (
    PLACE_FIELD_MASK,
    PLAN_FIELD_MASK,
    search_pages,
    search_request,
)

SOURCE_NAME = "google_places"
# Its dbt models read the month from the archive path (_snapshot_month).
SNAPSHOT_KEY_FORMAT = "%Y-%m"
PLAN_FILE = "plan.json"


def pay_for_plan(
    snapshot: str,
    archive: RawArchive,
    settings: GooglePlacesSettings,
    api: PlacesApi,
    plan: SweepPlan,
) -> dict[str, Download]:
    """Archives the plan, then each page of its searches to pay for, asked for with the fields
    the leads need. Fails before paying anything if no area is fetched or the plan is over budget.
    Returns each file's download by name; completing the snapshot is the caller's."""
    if not plan.fetches_any_area:
        raise ValueError(
            "No search area is to be fetched: give one the mode fetch in the "
            "google_search_scope seed"
        )
    if not plan.within_budget:
        raise ValueError(
            f"The sweep needs {plan.paid_requests} paid requests ({len(plan.to_pay)} searches), "
            f"more than google_places.max_paid_requests ({plan.max_paid_requests}); nothing "
            f"was paid"
        )
    archived = {PLAN_FILE: plan_download(plan, settings)}
    archive.save(SOURCE_NAME, snapshot, PLAN_FILE, archived[PLAN_FILE])

    def pay(search: PlannedSearch) -> list[Download]:
        request = search_request(search.query, search.tile.box, settings)
        # As many pages as the plan found, though the search may have grown since.
        return list(search_pages(api, request, PLACE_FIELD_MASK, search.pages))

    paid = in_parallel(pay, plan.to_pay, settings.workers)
    for search, downloads in zip(plan.to_pay, paid, strict=True):
        for page, download in enumerate(downloads, start=1):
            name = page_name(search.area, search.query, search.tile, page)
            archive.save(SOURCE_NAME, snapshot, name, download)
            archived[name] = download
    return archived


def plan_download(plan: SweepPlan, settings: GooglePlacesSettings) -> Download:
    """The plan as its snapshot keeps it: a paid page names one search that found a place, the
    plan every one."""
    paid = set(plan.to_pay)
    document = {
        "planned_at": plan.planned_at.isoformat(),
        "max_paid_requests": plan.max_paid_requests,
        "searches": [
            {
                "area": search.area.code,
                "query": search.query,
                "tile": search.tile.path,
                "pages": search.pages,
                "paid": search in paid,
                "place_ids": sorted(search.place_ids),
            }
            for search in plan.searches
        ],
    }
    return Download(
        url=settings.search_text_url,
        body=json.dumps(document, ensure_ascii=False).encode(),
        fetched_at=plan.planned_at,
        last_modified=None,
        request={"field_mask": PLAN_FIELD_MASK},
    )
