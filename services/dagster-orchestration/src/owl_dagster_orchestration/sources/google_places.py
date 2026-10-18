from functools import partial

from owl_config.configuration import Configuration
from owl_dagster_orchestration.asset_keys import transform_model_key
from owl_dagster_orchestration.snapshots import ArchivedSource, FetchPlan, SnapshotCadence
from owl_dagster_orchestration.sources.adapters import LiveAdapters
from owl_ingest.adapters.ducklake.search_areas import SEARCH_AREAS_MODEL, SEARCH_AREAS_SCHEMA
from owl_ingest.ports.raw_archive import RawArchive
from owl_ingest.settings import PipelineMode
from owl_ingest.sources.google_places.fetch import (
    SNAPSHOT_KEY_FORMAT,
    SOURCE_NAME,
    pay_for_plan,
)
from owl_ingest.sources.google_places.plan import SweepPlan, plan_fetch
from owl_ingest.sources.google_places.settings import (
    GooglePlacesSettings,
    load_google_places_settings,
)

# One box for Google's listings whatever fetches them: the Places API, or later a scraper, both
# feeding models/staging/google/.
GOOGLE_GROUP = "google"
SEARCH_AREAS = transform_model_key(SEARCH_AREAS_SCHEMA, SEARCH_AREAS_MODEL, PipelineMode.LIVE)


def google_places_source(
    config: Configuration, timezone: str, archive: RawArchive, live: LiveAdapters | None
) -> ArchivedSource:
    settings = load_google_places_settings(config)
    return ArchivedSource(
        name=SOURCE_NAME,
        cadence=SnapshotCadence(
            schedule=settings.snapshot_schedule, key_format=SNAPSHOT_KEY_FORMAT, timezone=timezone
        ),
        group=f"{GOOGLE_GROUP}/places",
        fetch_max_runtime_seconds=settings.fetch_max_runtime_seconds,
        # Every sweep is paid for, so its schedule is turned on by hand.
        schedule_running=False,
        fetch_deps=(SEARCH_AREAS,),
        plan=partial(plan_google_places, settings, archive, live) if live else None,
    )


def plan_google_places(
    settings: GooglePlacesSettings, archive: RawArchive, live: LiveAdapters
) -> FetchPlan:
    api = live.places_api()
    plan = plan_fetch(settings, api, live.search_areas.search_areas())
    pay = partial(pay_for_plan, archive=archive, settings=settings, api=api, plan=plan)
    return FetchPlan(
        paid_requests=plan.paid_requests,
        max_paid_requests=plan.max_paid_requests,
        summary=plan_summary(plan, settings),
        parts=plan_parts(plan, settings),
        pay=pay,
    )


def plan_summary(plan: SweepPlan, settings: GooglePlacesSettings) -> str:
    free = settings.free_paid_requests_a_month
    usd = settings.usd_per_paid_request
    paid = plan.paid_requests
    return (
        f"The next fetch pays for {paid:,} requests: USD {paid * usd:,.2f} if all are "
        f"billed, USD {max(paid - free, 0) * usd:,.2f} beyond the {free:,} free a month. "
        f"A fetch may pay for {plan.max_paid_requests:,}."
    )


def plan_parts(plan: SweepPlan, settings: GooglePlacesSettings) -> list[dict[str, object]]:
    usd = settings.usd_per_paid_request
    return [
        {
            "municipality": area.area.name,
            "code": area.area.code,
            "mode": area.area.mode.value,
            "places": area.places,
            "free requests": area.free_requests,
            "paid requests alone": area.paid_requests,
            "USD alone if billed": f"{area.paid_requests * usd:,.2f}",
        }
        for area in plan.areas
    ]
