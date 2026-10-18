import hashlib

from owl_testing.ingest import build_download


def test_a_downloads_size_and_sha256_are_of_its_bytes() -> None:
    # Arrange
    download = build_download(b"PK raw")

    # Act / Assert
    assert download.size == len(b"PK raw")
    assert download.sha256 == hashlib.sha256(b"PK raw").hexdigest()
