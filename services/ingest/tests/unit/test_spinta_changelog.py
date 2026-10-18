import itertools
import json
from datetime import UTC, datetime

import pytest

from owl_ingest.ports.downloader import Download
from owl_ingest.sources.spinta.changelog import Changelog
from owl_testing.ingest import FakeSpinta, change_entry

MODEL = "https://spinta.test/datasets/gov/vmi/mm_registras/MokesciuMoketojas"
PAGE_SIZE = 10_000
# Ids with gaps, as Spinta's have: 13 and 14 are missing.
CHANGES = [
    change_entry(10, "2026-09-16T23:57:00"),
    change_entry(11, "2026-09-16T23:57:00"),
    change_entry(12, "2026-09-17T23:57:18"),
    change_entry(15, "2026-09-17T23:57:46"),
    change_entry(16, "2026-09-19T00:03:50"),
]


def moment(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def held(pages: list[tuple[int, Download]]) -> list[object]:
    return [entry["_cid"] for _, page in pages for entry in json.loads(page.body)["_data"]]


@pytest.mark.parametrize(
    ("made_from", "boundary"),
    [
        pytest.param("2026-09-01T00:00:00", 1, id="before the first change"),
        pytest.param("2026-09-16T23:57:00", 1, id="at the first change"),
        pytest.param("2026-09-17T00:00:00", 12, id="between days"),
        pytest.param("2026-09-17T23:57:30", 13, id="before a gap"),
        pytest.param("2026-09-18T00:00:00", 16, id="after a gap"),
        pytest.param("2026-09-20T00:00:00", 17, id="after the newest change"),
    ],
)
def test_a_boundary_splits_the_changelog_at_a_moment(made_from: str, boundary: int) -> None:
    # Arrange
    changelog = Changelog(MODEL, FakeSpinta(MODEL, CHANGES), PAGE_SIZE)

    # Act
    found = changelog.boundary(moment(made_from))

    # Assert
    assert found == boundary
    made = {int(str(entry["_cid"])): moment(str(entry["_created"])) for entry in CHANGES}
    assert all(at < moment(made_from) for change, at in made.items() if change < found)
    assert all(at >= moment(made_from) for change, at in made.items() if change >= found)


def test_a_boundary_of_an_empty_changelog_is_its_start() -> None:
    # Act / Assert
    assert (
        Changelog(MODEL, FakeSpinta(MODEL), PAGE_SIZE).boundary(moment("2026-09-17T00:00:00")) == 1
    )


def test_a_window_holds_the_changes_made_in_it_and_no_later_one() -> None:
    # Arrange
    spinta = FakeSpinta(MODEL, CHANGES)

    # Act
    pages = list(
        Changelog(MODEL, spinta, PAGE_SIZE).pages(
            moment("2026-09-17T00:00:00"), moment("2026-09-18T00:00:00")
        )
    )

    # Assert
    assert [position for position, _ in pages] == [12]
    assert held(pages) == [12, 15]
    assert spinta.requested[-1] == f"{MODEL}/:changes/12?limit(2)"


def test_pages_follow_one_another_from_the_last_change_they_hold() -> None:
    # Arrange
    changelog = Changelog(MODEL, FakeSpinta(MODEL, CHANGES), page_size=2)

    # Act
    pages = list(changelog.pages(moment("2026-09-01T00:00:00"), moment("2026-09-20T00:00:00")))

    # Assert
    assert [position for position, _ in pages] == [1, 12, 16]
    assert held(pages) == [10, 11, 12, 15, 16]


def test_a_window_without_changes_has_no_pages() -> None:
    # Act
    pages = Changelog(MODEL, FakeSpinta(MODEL, CHANGES), PAGE_SIZE).pages(
        moment("2026-09-18T00:00:00"), moment("2026-09-19T00:00:00")
    )

    # Assert
    assert list(pages) == []


def test_consecutive_windows_search_the_changelog_once() -> None:
    # Arrange
    spinta = FakeSpinta(MODEL, CHANGES)
    changelog = Changelog(MODEL, spinta, PAGE_SIZE)
    days = ["2026-09-16", "2026-09-17", "2026-09-18", "2026-09-19", "2026-09-20"]

    # Act
    pages = [
        held(list(changelog.pages(moment(f"{start}T03:00:00"), moment(f"{end}T03:00:00"))))
        for start, end in itertools.pairwise(days)
    ]

    # Assert
    assert pages == [[10, 11], [12, 15], [16], []]
    searches = [url for url in spinta.requested if "select(" in url]
    reads = [url for url in spinta.requested if "select(" not in url]
    assert len(searches) <= 5  # the first window's start only
    # A page per window, and again for the two whose page reached past their end.
    assert len(reads) == 4 + 2


def test_days_before_the_next_change_cost_no_request() -> None:
    # Arrange
    spinta = FakeSpinta(MODEL, [CHANGES[0], CHANGES[-1]])
    changelog = Changelog(MODEL, spinta, PAGE_SIZE)
    changelog.boundary(moment("2026-09-17T03:00:00"))
    searched = len(spinta.requested)

    # Act
    empty = list(changelog.pages(moment("2026-09-17T03:00:00"), moment("2026-09-18T03:00:00")))
    after = list(changelog.pages(moment("2026-09-18T03:00:00"), moment("2026-09-19T03:00:00")))

    # Assert
    assert (empty, held(after)) == ([], [16])
    assert spinta.requested[searched:] == [f"{MODEL}/:changes/11?limit({PAGE_SIZE})"]
