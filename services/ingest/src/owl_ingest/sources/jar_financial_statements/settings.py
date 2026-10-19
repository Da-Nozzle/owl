from dataclasses import dataclass

from owl_config.configuration import Configuration


@dataclass(frozen=True, slots=True)
class JarFinancialStatementsSettings:
    # A dataset's CSV export, by its {dataset} path.
    download_url_template: str
    snapshot_schedule: str
    # Rows asked for in each request: each export is fetched in pages of this many.
    page_rows: int
    fetch_max_runtime_seconds: int


def load_jar_financial_statements_settings(config: Configuration) -> JarFinancialStatementsSettings:
    settings = JarFinancialStatementsSettings(
        download_url_template=config.get_str("jar_financial_statements.download_url_template"),
        snapshot_schedule=config.get_str("jar_financial_statements.snapshot_schedule"),
        page_rows=config.get_int("jar_financial_statements.page_rows"),
        fetch_max_runtime_seconds=config.get_int(
            "jar_financial_statements.fetch_max_runtime_seconds"
        ),
    )
    for key in ("page_rows", "fetch_max_runtime_seconds"):
        if getattr(settings, key) < 1:
            raise ValueError(
                f"'jar_financial_statements.{key}' must be at least 1: {getattr(settings, key)}"
            )
    return settings
