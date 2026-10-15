import pytest
from hypothesis import given
from hypothesis import strategies as st

from leads_ingest.months import Month

MONTHS = st.builds(Month, year=st.integers(1000, 9000), month=st.integers(1, 12))
MONTH_COUNTS = st.integers(-1200, 1200)


def test_month_key_round_trips() -> None:
    # Arrange
    key = "2026-09"

    # Act
    month = Month.parse(key)

    # Assert
    assert (month.year, month.month, month.key) == (2026, 9, "2026-09")


@given(month=MONTHS)
def test_any_months_key_parses_back_to_it(month: Month) -> None:
    # Act / Assert
    assert Month.parse(month.key) == month


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


@given(month=MONTHS, count=MONTH_COUNTS)
def test_going_forward_and_back_returns_to_the_month(month: Month, count: int) -> None:
    # Act / Assert
    assert month.plus(count).minus(count) == month


@given(month=MONTHS, count=st.integers(1, 1200))
def test_a_later_month_sorts_after(month: Month, count: int) -> None:
    # Act / Assert
    assert month < month.plus(count)


@pytest.mark.parametrize("key", ["2026-9", "2026-13", "202609", "2026-00"])
def test_malformed_month_keys_are_rejected(key: str) -> None:
    # Act / Assert
    with pytest.raises(ValueError, match=r"month|Month"):
        Month.parse(key)
