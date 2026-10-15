from dlt.common.configuration.specs import AwsCredentials
from dlt.common.storages.configuration import FilesystemConfiguration

from leads_lake.settings import LakeStorage


def filesystem_configuration(bucket_url: str, storage: LakeStorage) -> FilesystemConfiguration:
    """A location in the lake bucket, with the bucket's credentials."""
    return FilesystemConfiguration(
        bucket_url=bucket_url,
        credentials=AwsCredentials(
            aws_access_key_id=storage.key_id,
            aws_secret_access_key=storage.secret,
            region_name=storage.region,
            endpoint_url=endpoint_url(storage),
            s3_url_style=storage.url_style,
        ),
    )


def endpoint_url(storage: LakeStorage) -> str:
    """dlt reads whether to use TLS from the endpoint's scheme."""
    scheme = "https" if storage.use_ssl else "http"
    return f"{scheme}://{storage.endpoint}"
