import pytest

from leads_ingest.months import Month


def test_month_key_round_trips() -> None:
    # Arrange
    key = "2026-09"

    # Act
    month = Month.parse(key)

    # Assert
    assert (month.year, month.month, month.key) == (2026, 9, "2026-09")


def test_going_back_crosses_the_year_boundary() -> None:
    # Arrange
    month = Month(2026, 2)

    # Act
    earlier = month.minus(13)

    # Assert
    assert earlier == Month(2025, 1)


def test_going_forward_crosses_the_year_boundary() -> None:
    # Arrange
    month = Month(2026, 12)

    # Act
    later = month.plus(1)

    # Assert
    assert later == Month(2027, 1)


@pytest.mark.parametrize("key", ["2026-9", "2026-13", "202609", "2026-00"])
def test_malformed_month_keys_are_rejected(key: str) -> None:
    # Act / Assert
    with pytest.raises(ValueError, match=r"month|Month"):
        Month.parse(key)
