# pdv-dagster-enricher

## What this is

A small, containerized Dagster workflow that backfills `experiment_date`
metadata onto Girder `pdv_trace` items by walking the AIMD-L
`/aimdl/datafiles` endpoint, parsing dates out of file names, and
writing the metadata back via the Girder REST API. Intended as a
one-shot backfill — the upstream helix uploader writes
`experiment_date` on new uploads once it is deployed in production.

## Prerequisites

- Docker / Docker Compose (or a local Python ≥3.12 venv)
- A Girder API key with metadata-write permission on the target items
- The Girder id of the **collection or user** you want to enrich
  (folder ids are not accepted by `/aimdl/datafiles`)

## Quickstart

Run these in order. Do **not** skip the dry run.

1. **Set credentials.**
   ```
   cp .env.example .env
   # then edit .env to set GIRDER_API_URL and GIRDER_API_KEY
   ```

2. **Probe the endpoint** to confirm reachability and inspect a sample
   record before wiring up the asset:
   ```
   export BASE_PARENT_ID=...        # collection or user id
   export BASE_PARENT_TYPE=collection   # or "user"
   python probe_partition.py
   ```
   Confirm in the output that `offset advances: YES` and that the
   sampled item's `meta.experiment_date present` reflects the state you
   expect (typically `False` for backfill targets).

3. **Launch Dagster:**
   ```
   docker compose up
   ```
   Open http://localhost:3000.

4. **Materialize `enrich_pdv_trace_experiment_dates` with
   `run_config.preflight.yaml`.** Replace
   `PUT_COLLECTION_OR_USER_ID_HERE` first. Inspect the preflight summary
   and the first record in the logs.

5. **Then run with `run_config.dry_run.yaml`.** Confirm the per-page
   counters look sane and that `would_update` matches your expectations.

6. **Then `run_config.first_write.yaml`** — and only after manual
   inspection of the dry-run output. This is the first run that writes
   metadata to Girder.

## Design

See [DECISIONS.md](DECISIONS.md) for the locked design choices, the
endpoint contract notes, and the hard footguns.

## Scope note

This is a laptop-scale scaffold for one-off enrichment runs. It is
intentionally not a production deployment — there is no scheduler, no
sensor, no run storage backend, and no horizontal scaling. Treat the
single-container `docker compose up` as the supported topology.
