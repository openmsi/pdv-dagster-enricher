# pdv-dagster-enricher

## What this is

A small, containerized Dagster workflow that enriches Girder `pdv_trace`
items with `experiment_date` (and `igsn` when absent) by walking the
AIMD-L `/aimdl/partition` endpoint, parsing dates out of file/folder
names, and writing metadata back via the Girder REST API.

## Prerequisites

- Docker / Docker Compose
- A Girder API key with metadata-write permission on the target items
- The Girder id of the folder or collection you want to enrich

## Quickstart

Run these in order. Do **not** skip the dry run.

1. **Set credentials.**
   ```
   cp .env.example .env
   # then edit .env to set GIRDER_API_URL and GIRDER_API_KEY
   ```

2. **Probe the partition endpoint** to discover the filter and pagination
   contract before wiring up the asset:
   ```
   docker compose run --rm dagster python probe_partition.py
   ```
   (or run `probe_partition.py` directly with the env vars exported).
   Note which mode returned non-empty results — you will set
   `partition_filter_mode` accordingly in the run configs.

3. **Launch Dagster:**
   ```
   docker compose up
   ```
   Open http://localhost:3000.

4. **Materialize `enrich_pdv_trace_experiment_dates` with
   `run_config.preflight.yaml`.** Replace
   `PUT_FOLDER_OR_COLLECTION_ID_HERE` first. Inspect the preflight summary
   and the first normalized record in the logs.

5. **Then run with `run_config.dry_run.yaml`.** Confirm the per-page
   counters look sane and that `would_update` matches your expectations.

6. **Then `run_config.first_write.yaml`** — and only after manual
   inspection of the dry-run output. This is the first run that writes
   metadata to Girder.

## Design

See [DECISIONS.md](DECISIONS.md) for the locked design choices, the
open questions the probe + preflight resolve, and the hard footguns.

## Scope note

This is a laptop-scale scaffold for one-off enrichment runs. It is
intentionally not a production deployment — there is no scheduler, no
sensor, no run storage backend, and no horizontal scaling. Treat the
single-container `docker compose up` as the supported topology.
