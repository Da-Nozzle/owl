from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol


@dataclass(frozen=True, slots=True)
class BoundingBox:
    """Latitude and longitude bounds, WGS 84."""

    south: float
    west: float
    north: float
    east: float

    def __post_init__(self) -> None:
        if not (self.south < self.north and self.west < self.east):
            raise ValueError(f"{self} is not a rectangle south-west to north-east")


class SearchMode(StrEnum):
    PRICE = "price"  # planned for free, to show what fetching it would cost
    FETCH = "fetch"  # planned, and paid for by the monthly fetch


@dataclass(frozen=True, slots=True)
class SearchArea:
    """A municipality to search Google in, by its Address Register code."""

    code: int
    name: str
    mode: SearchMode
    box: BoundingBox


class SearchAreaReader(Protocol):
    def search_areas(self) -> list[SearchArea]:
        """The areas with a search mode, by code."""
        ...
