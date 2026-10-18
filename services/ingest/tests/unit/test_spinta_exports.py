import pytest

from leads_ingest.sources.spinta.exports import require_rows_with_ids


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(b"_type,_id,kodas\n", id="header only"),
        pytest.param(b"_type,_id,kodas\n\n", id="header and a blank line"),
        pytest.param(b"_type,kodas\nForma,310\n", id="no _id column"),
        pytest.param(b"", id="empty"),
        pytest.param(b"<html><body>500 Internal Server Error</body></html>\n", id="error page"),
    ],
)
def test_an_export_without_ids_or_rows_is_rejected(body: bytes) -> None:
    # Act / Assert
    with pytest.raises(ValueError, match=r"Forma\.csv"):
        require_rows_with_ids("Forma.csv", body)


def test_an_export_with_ids_and_a_row_is_accepted() -> None:
    # Act / Assert
    require_rows_with_ids("JuridinisAsmuo.csv", b'_type,_id,ja_kodas\nJuridinisAsmuo,a1,"1"\n')
