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
