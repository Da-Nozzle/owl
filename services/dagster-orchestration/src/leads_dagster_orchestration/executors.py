from collections.abc import Iterator, Sequence

import dagster as dg
from dagster._core.events import DagsterEvent
from dagster._core.execution.context.system import PlanOrchestrationContext
from dagster._core.execution.plan.plan import ExecutionPlan
from dagster._core.execution.retries import RetryMode
from dagster._core.execution.step_dependency_config import StepDependencyConfig


class SequentialInProcessExecutor(dg.Executor):
    """Runs a run's steps in the run's own process when no two of them could run at once, and
    each step in a process of its own otherwise. A step's process loads Python and the
    definitions again (about 5 s), which buys nothing when the steps run one after another
    anyway, as in a backfill's run of one dbt step, or the archive and then its dbt step."""

    def __init__(self, in_process: dg.Executor, multiprocess: dg.Executor) -> None:
        self._in_process = in_process
        self._multiprocess = multiprocess

    @property
    def retries(self) -> RetryMode:
        return self._multiprocess.retries

    @property
    def step_dependency_config(self) -> StepDependencyConfig:
        return self._multiprocess.step_dependency_config

    def execute(
        self, plan_context: PlanOrchestrationContext, execution_plan: ExecutionPlan
    ) -> Iterator[DagsterEvent]:
        sequential = is_sequential(execution_plan.get_steps_to_execute_by_level())
        executor = self._in_process if sequential else self._multiprocess
        return executor.execute(plan_context, execution_plan)


def is_sequential(steps_by_level: Sequence[Sequence[object]]) -> bool:
    """Whether a plan's steps form a chain, each waiting for the one before it."""
    return all(len(level) <= 1 for level in steps_by_level)


@dg.executor(
    name="sequential_in_process",
    config_schema=dg.multiprocess_executor.config_schema,
    requirements=dg.multiple_process_executor_requirements(),
)
def sequential_in_process_executor(init_context: dg.InitExecutorContext) -> dg.Executor:
    """Takes the multiprocess executor's configuration, which holds the in-process executor's."""
    return SequentialInProcessExecutor(
        in_process=dg.in_process_executor.executor_creation_fn(init_context),
        multiprocess=dg.multiprocess_executor.executor_creation_fn(init_context),
    )
