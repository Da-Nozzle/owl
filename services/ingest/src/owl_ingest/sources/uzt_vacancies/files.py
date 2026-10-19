import time
from collections.abc import Callable

from owl_ingest.ports.downloader import Downloader, StreamedDownload, StreamingDownloader
from owl_ingest.ports.raw_archive import RawArchive
from owl_ingest.sources.spinta.changelog import last_change
from owl_ingest.sources.spinta.exports import csv_export_url
from owl_ingest.sources.spinta.pages import ChangedExportError, archive_pages
from owl_ingest.sources.uzt_vacancies.settings import UztVacanciesSettings

SOURCE_NAME = "uzt_vacancies"
# Its dbt models read the month from the archive path (_snapshot_month).
SNAPSHOT_KEY_FORMAT = "%Y-%m"
# The export is about 2.3 GB as sent, so it is fetched in pages, each compressed as it is
# written to about a fifth: Vieta.csv.gz, Vieta.2.csv.gz, ... (spinta.pages).
VACANCIES_NAME = "Vieta"
# The export's columns in their published order, but for Spinta's _base and the employer's
# contact person with their phone numbers and e-mail address (darbdavio_kontaktinis_asmuo,
# darbdavio_tel_nr, darbdavio_mob_nr, darbdavio_el_pastas): they name people and nothing here
# uses them, so they are never fetched and the archive holds no one's contact details. _type and
# _page (published as _page.next) are what the paging reads. A column the portal adds is not
# fetched until it is listed here.
COLUMNS = (
    "_type",
    "_id",
    "_revision",
    "_page",
    "darbo_vietos_id",
    "statusas",
    "ikelimo_data",
    "prelim_darbo_uzmokestis",
    "vid_darbo_uzmokestis",
    "maks_darbo_uzmokestis",
    "valiuta",
    "uzmokescio_komentaras_lt",
    "profesijos_grupes_kodas",
    "profesijos_grupes_pav",
    "profesijos_kodas",
    "profesijos_pareigybes_pav",
    "kontrakto_tipas",
    "darbo_aprasymas_lt",
    "galioja_nuo",
    "galioja_iki",
    "ar_aktuali_siandien",
    "ar_uzpildyta",
    "pageidaujama_darbo_pradzia",
    "darbo_vietu_skaicius",
    "darbo_vietos_adresas",
    "darbo_vietos_sav_pav",
    "registravimo_pagrindo_pav",
    "registravimo_budo_pav",
    "pageidavimo_pateikimo_pav",
    "ar_papildomai_remia",
    "ar_darbina_po_mokymu",
    "ar_apmoka_keliones",
    "ar_apgyvendina",
    "ar_maitina",
    "rizikos_lt",
    "jar_kodas",
    "darbdavys",
    "teisinio_statuso_pav",
    "teisines_formos_pav",
    "imones_iregistravimas",
    "darbdavio_bustine",
    "susisiekimo_budas",
    "reik_darbo_patirtis",
    "reik_kompetencijos_lt",
    "reik_gebejimai",
    "reik_issilavinimo_pav",
    "reik_mok_progr_pav",
    "ar_moksleiviams",
    "ar_iki_18",
    "ar_studentams",
    "ar_kariams",
    "ar_ukrainieciams",
    "ar_turintiems_negalia",
)


def archive_vacancies(
    snapshot: str,
    archive: RawArchive,
    settings: UztVacanciesSettings,
    downloader: Downloader,
    streaming: StreamingDownloader,
    *,
    wait: Callable[[float], None] = time.sleep,
) -> dict[str, StreamedDownload]:
    """Saves the export to the snapshot in pages of `settings.page_rows` rows, all of the version
    of the changelog's newest change. The portal's cache may send a count or a page from before
    it, even months old, while it makes a new one: the fetch is then asked for again from its count
    after a wait, the pages already saved written again. Completing the snapshot is the caller's."""
    url = csv_export_url(settings.vacancies_url)
    stale: list[str] = []
    for attempt in range(settings.export_attempts):
        if attempt:
            wait(settings.export_wait_seconds)
        _, newest = last_change(settings.vacancies_url, downloader)
        if newest is None:
            stale.append("the changelog holds no change")
            continue
        try:
            return archive_pages(
                SOURCE_NAME,
                snapshot,
                archive,
                VACANCIES_NAME,
                url,
                settings.page_rows,
                streaming,
                columns=COLUMNS,
                version=newest.revision,
            )
        except ChangedExportError as error:
            stale.append(str(error))
    raise ValueError(
        f"The vacancies' export never matched their changelog's newest change: {stale}"
    )
