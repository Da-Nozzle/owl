# Results: August 2026

Output of one national run of the risk-model proof of concept, built on 2026-10-19 from the live
archive. The month is August 2026, the newest one Sodra had published.

| File | What it holds |
|---|---|
| `rm_wage_floor_fired.csv` | The 376 companies in Lithuania whose declared average wage sat at the minimum wage (0.95–1.05×) for six months running while their peers paid more, with the evidence and the explanation the model wrote |
| `company_risk_vilnius.csv` | The scoring (`fct_company_risk`) for the 882 Vilnius companies in the three leads categories that the model could judge |

**These are prompts to look closer, not accusations.** The thresholds were calibrated on one
month of data and were never checked against anything real. All the data is open data.

**The companies are redacted, because I did not want to be sued.** Flagging a named business for
envelope wages on the strength of an unvalidated model is how you end up in court, so every
company code and name is `<REDACTED>`. So are the figures that would find a company again in
Sodra's own open data: its headcount (`insured_count`), and its average wage for the month and
that wage as a multiple of the minimum, in `explanation` and `evidence`. Everything else is as the
model wrote it.

## The run

The run was `owl-dbt-dev --lake live build` of the seeds, the company-month profile, `rm_wage_floor`
and the two scoring marts, reading every source from the live lake. All 85 nodes passed: 4 seeds,
6 models, 65 data tests and 10 unit tests. dbt took 56.6 seconds (78 seconds wall). It's slow due to the very conservative
threads and meory limit.

| Model | Rows | Time |
|---|---|---|
| `int_company_months` | 4,534,368 (260,162 companies, 2025-01 to 2026-08) | 33.3 s |
| `rm_wage_floor__months` | | 2.1 s |
| `rm_wage_floor__windows` | | 2.0 s |
| `rm_wage_floor_v1` | 39,836 in 2026-08, 37,331 of them with a peer percentile | 6.2 s |
| `fct_score_components` | | 3.2 s |
| `fct_company_risk` | 39,836 in 2026-08 | 4.1 s |

The sources were as of JAR 2026-08-21 and Sodra 2026-09-29 (the date Sodra's file was published).

## What it shows

- **Tiers in 2026-08:**
  - 39,653 companies are "insufficient evidence". This is by design: a lead needs at least two
    risk models with data, and only one exists.
  - 183 are excluded: 170 have fewer than 6 months in Sodra, and 13 are in liquidation or
    bankruptcy.
- **`rm_wage_floor` fired per month:**
  - 296–355 companies a month from 2025-06 to 2025-12;
  - 216–242 from January to May 2026, after the minimum wage rose 11%;
  - 351–376 since June 2026.
  
  Months before 2025-06 have fewer than six months of history in the archive, so the model can't
  fire for them.
- **Vilnius, three leads categories:**

  | Category | Judged | Fired |
  |---|---|---|
  | Cafés and restaurants | 545 | 6 |
  | Car repair | 268 | 7 |
  | Hair and beauty | 69 | 2 |

  Most hair and beauty employers have 3 or fewer insured, so Sodra publishes no wage for them, and
  the model can't judge them.
- **An explanation as the model writes it:** "Declared average wage EUR &lt;REDACTED&gt; in 2026-08,
  &lt;REDACTED&gt;× the minimum wage; 0.95–1.05× it in 6 of the last 6 months; 100% of peers (EVRK 2.1 46.89 in
  Lithuania, 61 companies) are further from it."

See [DECISIONS.md](../../DECISIONS.md), "A proof-of-concept fraud detection model in SQL", for how
it works and its known issues.
