# Ingestion throughput report

Generated 2026-10-07 19:44 UTC by `measure_throughput.py` directly from the database.

- Table `stream.fact_event_stream`: 1,801,280 rows, 513 MB
- Data from 2026-10-03 21:51 to 2026-10-07 19:41 UTC

## Per-minute counts, last 6 hours (359 complete minutes)

| Metric | Value |
|---|---|
| Average rows/min | 320.0 |
| Median | 320 |
| Min / Max | 300 / 340 |
| Std. deviation | 8.6 |
| Minutes at or above 100/min | 359 of 359 |
| Minutes with no data | 0 |

## Live sample

640 rows arrived in 120 s, which is **320 rows/min**.

`ts` is the event time stamped by the generator, so the per-minute table counts readings per minute of event time. The live sample measures actual arrival at the database.
