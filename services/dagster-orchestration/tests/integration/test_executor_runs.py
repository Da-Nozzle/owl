import dagster as dg
import pytest

from owl_dagster_orchestration.executors import sequential_in_process_executor


@dg.asset
def archive() -> None:
    pass


@dg.asset(deps=[archive])
def raw() -> None:
    pass


@dg.asset
def other_source() -> None:
    pass


DEFINITIONS = dg.Definitions(
    assets=[archive, raw, other_source],
    jobs=[
        dg.define_asset_job("chain", [archive, raw]),
        dg.define_asset_job("side_by_side", [archive, other_source]),
    ],
    executor=sequential_in_process_executor,
)


def chain_job() -> dg.JobDefinition:
    return DEFINITIONS.resolve_job_def("chain")


def side_by_side_job() -> dg.JobDefinition:
    return DEFINITIONS.resolve_job_def("side_by_side")


@pytest.fixture
def instance(postgres_instance: dg.DagsterInstance) -> dg.DagsterInstance:
    """As dev's, so each step's process writes its events to Postgres."""
    return postgres_instance


def executor_message(job_fn: object, instance: dg.DagsterInstance) -> str:
    """Runs the job as a launched run does, through its executor, and says which one ran it."""
    result = dg.execute_job(dg.reconstructable(job_fn), instance)
    assert result.success
    return next(
        event.message or ""
        for event in result.all_events
        if event.is_engine_event and (event.message or "").startswith("Executing steps")
    )


def test_a_chain_of_steps_runs_in_the_runs_process(instance: dg.DagsterInstance) -> None:
    # Act
    message = executor_message(chain_job, instance)

    # Assert
    assert message.startswith("Executing steps in process")


def test_steps_that_could_run_at_once_each_get_a_process(instance: dg.DagsterInstance) -> None:
    # Act
    message = executor_message(side_by_side_job, instance)

    # Assert
    assert message.startswith("Executing steps using multiprocess executor")
