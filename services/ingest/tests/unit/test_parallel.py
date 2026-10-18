import time

import pytest

from leads_ingest.parallel import expand_in_parallel, in_parallel


def test_results_come_in_the_items_order_whichever_ends_first() -> None:
    # Arrange
    def slower_first(item: int) -> int:
        time.sleep(0.01 * (3 - item))
        return item

    # Act
    results = list(in_parallel(slower_first, [0, 1, 2], workers=3))

    # Assert
    assert results == [0, 1, 2]


def test_items_after_a_failure_are_not_started() -> None:
    # Arrange
    started: list[int] = []

    def failing(item: int) -> int:
        started.append(item)
        raise ConnectionError(item)

    # Act / Assert
    with pytest.raises(ConnectionError):
        list(in_parallel(failing, [0, 1, 2], workers=1))
    assert started == [0]


def test_every_item_a_call_returns_is_called_too() -> None:
    # Arrange: each number below 4 returns its two children in a binary tree.
    def children(item: int) -> tuple[int, list[int]]:
        return item, [2 * item, 2 * item + 1] if item < 4 else []

    # Act
    results = expand_in_parallel(children, [1], workers=2)

    # Assert
    assert sorted(results) == [1, 2, 3, 4, 5, 6, 7]
