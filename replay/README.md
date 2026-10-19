# Replay sample

Raw source files for keyless runs (the `replay` code location reads this directory instead of
calling the sources). The layout matches the live raw archive: `<source>/<snapshot>/<file>`, each
file with a `.meta.json` sidecar with where and when it was fetched, and each snapshot's manifest
at `<source>/_snapshots/<snapshot>.json`. The replay location's partitions are the snapshots with
a manifest, so a snapshot added here, with its manifest, becomes a partition and is built.

## sodra/2026-09

Sodra's yearly files `monthly-2025.csv.zip` and `monthly-2026.csv.zip`, as downloaded on
2026-10-14, cut down to rows with municipality `Vilniaus m. sav.` and an activity code in the
three target categories (EVRK prefixes 5610, 5611, 5612, 5630, 9602, 9621, 9622, 4520, 9531,
after restoring the leading zero Sodra's numeric export drops). Kept lines are byte-for-byte
as published, header and BOM included. 2025: 18,045 rows; 2026: 12,307 rows.

## jar/2026-09

The register's exports from get.data.gov.lt (`rc/jar/.../:format/csv`), as fetched on 2026-10-17
at 16:19 UTC by the `jar` source's own fetch: `Forma.csv` (168 legal forms) and `Statusas.csv` (31
statuses) whole, `JuridinisAsmuo.csv` cut down to the 1,847 entities whose code is a `jarCode` in
`sodra/2026-09` (all of them are in the register), so every Sodra employer in the sample has its
JAR row, and `Buveine.csv` cut down to those entities' seats (1,787; the other 60 have none, as the
seats dataset has not changed since 2025-09-03). Kept lines are byte-for-byte as published, header
included. The register's export was last changed on 2026-08-21 and holds registrations up to
2026-07-31. Each sidecar and the manifest record the URL, fetch time and Last-Modified of the full
download, and the size and SHA-256 of the file kept here.

## address_register/2026-09

The Address Register's exports (`rc/ar/.../:format/csv`), as fetched on 2026-10-17 at 16:20 UTC by
the `address_register` source's own fetch, cut down to the addresses the seats in `jar/2026-09`
point at (1,309): `Pastatas.csv` holds those that are buildings' or land plots' and the buildings
the premises among them are in (1,202), `Patalpa.csv` those that are premises' (838), `Gatve.csv`
and `GyvenamojiVietove.csv` the streets (462) and settlements (14) of those buildings, and
`Savivaldybe.csv` all 60 municipalities. Kept lines are byte-for-byte as published, header
included; the sidecars and manifest are recorded as for `jar/2026-09`. Every seat in the sample
resolves to an address: 1,742 of the 1,847 entities have one, 1,071 of them at premises, 1,728 in
Vilnius city.

## vmi_monthly_register/2026-09

The VMI taxpayer register as fetched on 2026-10-17 by the `vmi_monthly_register` source's own fetch:
`MokesciuMoketojas.csv` cut down to the 6,621 rows of the 1,846 companies whose code is a `jarCode`
in `sodra/2026-09` (one of Sodra's 1,847 is not in the register), lines byte for byte as published,
header included; `EkonominesVeiklosRusis.csv` (the EVRK 2 classifier) whole; and
`last-change.json`, the changelog's newest change then (id 27,416,165, made 2026-10-16 00:00:08 UTC),
whose revision is the export's ETag.

## vmi_daily_changes/2026-09-01 to vmi_daily_changes/2026-09-30

The register's changelog for September 2026, one snapshot a day as the `vmi_daily_changes`
source fetched them on 2026-10-17 (each the changes made from 06:00 Vilnius time the day before),
cut down to the changes of the sample's register rows: 491 changes on 10 of the 30 days (18
inserts, the rest patches, no deletes: a deleted row is not in the export, so its company is not
known). Days with none have a manifest and no page. The pages' entries are kept as the portal sent them, but each page is
re-serialized (`{"_data":[...]}`, compact, UTF-8) after the cut, so its bytes are not the portal's.
They are all older than the export, so in replay they are history: `stg_vmi__changes` holds them,
and `stg_vmi__current_register` is the export as it is.

## google_places/2026-09

A real sweep of the Places API by the `google_places` source's own fetch on 2026-10-17, not cut
from a larger one: "kavinė", "kirpykla" and "autoservisas" over a box around the Old Town and
Naujamiestis (54.672–54.692 N, 25.255–25.295 E) instead of all of Vilnius, with the configured
tile sizes. 116 free requests planned it and 81 paid ones fetched the searches holding every place
found: 81 pages kept byte for byte as the API sent them, each sidecar with the request it answered,
holding 803 listings (one search has a page more than its plan, from before the fetch stopped at
the planned pages). The pages were renamed the same day with their search area's code
(`places-13-...`, Vilnius), as pages are named since areas were added; the manifest's names with them. Google's terms allow only place ids to be stored indefinitely, so this sample is
not to be published as it is.

## vmi_tax_arrears/2024-11 to vmi_tax_arrears/2026-09

The changelog of VMI's list of companies with tax arrears (`vmi/ja_nepriemokos/NepriemokosSuma`),
one snapshot a month from its first change (2024-11-18) to the 2026-09 tick, as the
`vmi_tax_arrears` source fetched them on 2026-10-18 (each the changes made from the 21st, 06:00
Vilnius time, of the month before). They are cut down to every change of the list rows of the 251
companies of `jar/2026-09` whose code is divisible by 8: 13,483 changes of 2,682 rows (all 1,847
companies' would be 29 MB). A row's company is known from its insert, so a row is kept whole or not
at all. Its staging has 184 companies on the list at some tick, 94 at 2026-09's. Pages with none of
their changes are left out, from the manifests too. The kept pages' entries are as the portal sent
them, re-serialized (`{"_data":[...]}`, compact, UTF-8) after the cut. Sidecars and manifests
record the full download.

## vmi_taxes_paid/2025-06 to vmi_taxes_paid/2026-09

The changelog of the taxes legal entities paid to VMI (`vmi/ja_mokesciai/Moketojas/:changes`),
one snapshot a month as the `vmi_taxes_paid` source's own fetch took them on 2026-10-18 (each the
changes made from 06:00 Vilnius time on the 21st of the month before), cut down to one in eight of
the sample's companies: the 251 entities in `jar/2026-09` whose code is divisible by 8, and every
change of the rows that refer to one of them (`mm_kodas`), their inserts, patches and deletes.
7,093 changes in 208 pages, 2.9 MB; 2025-10 has no load, so it has a manifest and no page. 244 of
the 251 paid something in some month, 3,777 rows of `stg_vmi_taxes_paid__monthly` from 2025-04 to
2026-08. Pages are re-serialized after the cut (`{"_data":[...]}`, compact, UTF-8), as
`vmi_daily_changes`' are; each sidecar and manifest keeps the full page's URL, fetch time,
Last-Modified and ETag, with the size and SHA-256 of the file kept here.

## uzt_vacancies/2026-09

The Employment Service's vacancy export (`uzt/ldv/Vieta`, `:format/csv` with the columns the
source selects) as fetched on 2026-10-18 at 21:28 UTC by the `uzt_vacancies` source's own fetch,
cut down to the vacancies of one in eight of `jar/2026-09`'s companies (the 251 whose code is
divisible by 8, as the VMI tax samples are cut): 4,559 vacancies since 1990 of 160 employers,
3,357 of them (of 145 companies) posted since 2015. Records are kept byte for byte as published
(multi-line descriptions included), header included, and gzipped as the archive stores the export (530 KB, 3.1 MB as CSV).
The sidecar and manifest record the URL, fetch time, Last-Modified and ETag of the full download,
and the size and SHA-256 of the CSV kept here, before compression.

## jar_financial_statements/2026-09

The register's financial statements (`rc/jar/.../:format/csv`) as the live snapshot `2026-09`
holds them, cut down to the 251 entities in `jar/2026-09` whose code is divisible by 8 (one in
eight of the sample's companies), lines byte for byte as published, header included: the income
statements' 13 pages as fetched page by page on 2026-10-19 at 05:55–06:32 UTC (9,079 rows), and
the balance sheets (13,496 rows), the non-filers (70) and the late filers (12) as exported whole
on 2026-10-18 at 19:25–19:29 UTC. Each file is kept gzip-compressed, as in the live archive; the
sidecars and manifest are recorded as for `jar/2026-09`, the size and SHA-256 being of the file
kept here before compression. 225 of the 251 companies filed statements.
