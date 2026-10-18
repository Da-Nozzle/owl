import pytest

from leads_ingest.ports.search_areas import BoundingBox
from leads_ingest.sources.google_places.tiles import Tile

# About 2.2 km north to south and 2.6 km west to east.
BOX = BoundingBox(south=54.68, west=25.26, north=54.70, east=25.30)


def test_a_tiles_quarters_cover_it_and_share_their_edges() -> None:
    # Arrange
    tile = Tile.whole(BOX)

    # Act
    quarters = tile.quarters()

    # Assert
    assert [(quarter.path, quarter.box) for quarter in quarters] == [
        ("r0", BoundingBox(54.68, 25.26, 54.69, 25.28)),
        ("r1", BoundingBox(54.68, 25.28, 54.69, 25.30)),
        ("r2", BoundingBox(54.69, 25.26, 54.70, 25.28)),
        ("r3", BoundingBox(54.69, 25.28, 54.70, 25.30)),
    ]
    assert round(tile.size_meters) == 2_574


def test_a_box_must_run_south_west_to_north_east() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="south-west to north-east"):
        BoundingBox(south=54.83, west=25.02, north=54.57, east=25.48)
