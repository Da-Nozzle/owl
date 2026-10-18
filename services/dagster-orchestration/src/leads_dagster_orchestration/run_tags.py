MODE_TAG = "leads/mode"
LAKE_TAG = "leads/lake"
TRIGGER_TAG = "leads/trigger"
# Dagster's own (dagster._core.storage.tags). A re-execution keeps its parent's schedule and
# automation tags.
SCHEDULE_TAG = "dagster/schedule_name"
AUTOMATION_TAG = "dagster/from_automation_condition"
# Run monitoring fails a run that takes longer than this many seconds.
MAX_RUNTIME_TAG = "dagster/max_runtime"
