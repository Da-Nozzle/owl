from owl_lake.settings import LakeStorage

# DuckDB's URL_STYLE names, as the lake's config uses them, to botocore's.
ADDRESSING_STYLES = {"path": "path", "vhost": "virtual"}


def s3fs_options(storage: LakeStorage) -> dict[str, object]:
    """From the same settings as DuckDB's secret, so Python and DuckDB reach the bucket alike."""
    return {
        "key": storage.key_id,
        "secret": storage.secret,
        "endpoint_url": endpoint_url(storage),
        "client_kwargs": {"region_name": storage.region},
        "config_kwargs": {
            "s3": {"addressing_style": addressing_style(storage.url_style)},
            # botocore checks checksums an S3-compatible store (s3mock) may not send.
            "request_checksum_calculation": "when_required",
            "response_checksum_validation": "when_required",
        },
        # A cached listing would hide snapshots completed since.
        "use_listings_cache": False,
    }


def endpoint_url(storage: LakeStorage) -> str:
    # botocore reads whether to use TLS from the scheme.
    scheme = "https" if storage.use_ssl else "http"
    return f"{scheme}://{storage.endpoint}"


def addressing_style(url_style: str) -> str:
    if url_style not in ADDRESSING_STYLES:
        raise ValueError(
            f"'lake.s3.url_style' is {url_style!r}, expected one of {sorted(ADDRESSING_STYLES)}"
        )
    return ADDRESSING_STYLES[url_style]
