from collections.abc import Mapping
from typing import Protocol

# The name the lake is attached under, which a query names its tables by (lake.<schema>.<table>).
LAKE = "lake"


class LakeReader(Protocol):
    """Reads tables the pipeline built into the lake, for a fetch that depends on them: Google's
    search areas are a dbt model."""

    def rows(self, sql: str) -> list[Mapping[str, object]]: ...
