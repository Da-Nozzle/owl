from owl_dagster_orchestration.executors import is_sequential


def test_steps_that_wait_for_each_other_are_sequential() -> None:
    # Arrange
    one_step = [["transform_sodra"]]
    chain = [["archive_sodra"], ["transform_sodra"]]

    # Act / Assert
    assert is_sequential(one_step)
    assert is_sequential(chain)
    assert is_sequential([])


def test_steps_that_could_run_at_once_are_not_sequential() -> None:
    # Arrange
    two_sources = [["archive_sodra", "archive_jar"], ["transform_sodra"]]

    # Act / Assert
    assert not is_sequential(two_sources)
