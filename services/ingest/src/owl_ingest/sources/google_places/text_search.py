import json
from collections.abc import Iterator, Mapping

from owl_ingest.ports.downloader import Download
from owl_ingest.ports.places_api import PlacesApi
from owl_ingest.ports.search_areas import BoundingBox
from owl_ingest.sources.google_places.settings import GooglePlacesSettings

# The API's documented page size and result cap. A third page may still carry a token, so no page
# past the cap is asked for.
PAGE_SIZE = 20
RESULT_CAP = 60
MAX_PAGES = RESULT_CAP // PAGE_SIZE
# Free (Text Search Essentials IDs Only).
PLAN_FIELD_MASK = "places.id,nextPageToken"
# Billed as Text Search Enterprise for the rating, review count, opening hours, website and phone;
# the Pro fields cost nothing more beside them. No Atmosphere field, which is billed higher still.
PLACE_FIELD_MASK = ",".join(
    (
        "places.id",
        "places.displayName",
        "places.formattedAddress",
        "places.addressComponents",
        "places.location",
        "places.types",
        "places.primaryType",
        "places.businessStatus",
        "places.googleMapsUri",
        "places.rating",
        "places.userRatingCount",
        "places.regularOpeningHours",
        "places.websiteUri",
        "places.nationalPhoneNumber",
        "nextPageToken",
    )
)
# About a centimetre, so two quarters round the edge they share alike.
COORDINATE_DECIMALS = 7


def search_request(
    query: str, box: BoundingBox, settings: GooglePlacesSettings
) -> dict[str, object]:
    return {
        "textQuery": query,
        "pageSize": PAGE_SIZE,
        "languageCode": settings.language_code,
        "regionCode": settings.region_code,
        "locationRestriction": location_restriction(box),
    }


def location_restriction(box: BoundingBox) -> dict[str, object]:
    """Places on the box's edge are inside it."""

    def point(latitude: float, longitude: float) -> dict[str, float]:
        return {
            "latitude": round(latitude, COORDINATE_DECIMALS),
            "longitude": round(longitude, COORDINATE_DECIMALS),
        }

    return {"rectangle": {"low": point(box.south, box.west), "high": point(box.north, box.east)}}


def search_pages(
    api: PlacesApi, request: Mapping[str, object], field_mask: str, pages: int = MAX_PAGES
) -> Iterator[Download]:
    """Up to `pages` pages of a search (at most the cap's), each after the first asked for with
    the previous page's token."""
    page_request = dict(request)
    for page in range(1, min(pages, MAX_PAGES) + 1):
        download = api.search_text(page_request, field_mask)
        yield download
        if not has_next_page(download) or page == pages:
            return
        # The API requires a later page's request to repeat every parameter.
        page_request = {**request, "pageToken": page_document(download)["nextPageToken"]}


def has_next_page(download: Download) -> bool:
    return bool(page_document(download).get("nextPageToken"))


def page_document(download: Download) -> Mapping[str, object]:
    """`{}` when nothing was found."""
    document = json.loads(download.body)
    if not isinstance(document, Mapping):
        raise ValueError(f"Not a Text Search page: {download.body[:200]!r}")
    return document


def page_places(download: Download) -> list[object]:
    places = page_document(download).get("places", [])
    if not isinstance(places, list):
        raise ValueError(f"Not a Text Search page: {download.body[:200]!r}")
    return places
