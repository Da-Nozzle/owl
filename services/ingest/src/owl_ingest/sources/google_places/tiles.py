import math
from dataclasses import dataclass
from typing import Self

from owl_ingest.ports.search_areas import BoundingBox

METRES_PER_DEGREE = 111_320


@dataclass(frozen=True, slots=True)
class Tile:
    """Part of a search area's box, named by its path from the whole box, `r`: each split appends
    the quarter's digit, 0 south-west, 1 south-east, 2 north-west, 3 north-east."""

    path: str
    box: BoundingBox

    @classmethod
    def whole(cls, box: BoundingBox) -> Self:
        return cls("r", box)

    @property
    def size_meters(self) -> float:
        """The longer of its sides."""
        box = self.box
        middle = math.radians((box.south + box.north) / 2)
        height = (box.north - box.south) * METRES_PER_DEGREE
        width = (box.east - box.west) * METRES_PER_DEGREE * math.cos(middle)
        return max(height, width)

    def quarters(self) -> list[Self]:
        box = self.box
        latitude, longitude = (box.south + box.north) / 2, (box.west + box.east) / 2
        corners = (
            BoundingBox(box.south, box.west, latitude, longitude),
            BoundingBox(box.south, longitude, latitude, box.east),
            BoundingBox(latitude, box.west, box.north, longitude),
            BoundingBox(latitude, longitude, box.north, box.east),
        )
        return [type(self)(f"{self.path}{index}", corner) for index, corner in enumerate(corners)]
