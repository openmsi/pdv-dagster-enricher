# Decisions

Design choices for this scaffold. Revisit only with explicit reason.

## Locked decisions

1. **Single `@asset` with rich output metadata, not a multi-asset graph.** The
   work is one logical unit (paginate, parse, enrich); splitting into a
   graph adds materialization plumbing and IO managers without buying
   anything observable that we don't already get from per-page log lines and
   `context.add_output_metadata`.

2. **`parse_date` is copied verbatim from
   `xarthisius/girder-consumers/common/base.py`** with an attribution
   comment in `app/definitions.py`. Not pip-installed and not pulled in
   as a git submodule, because that repo is not packaged and submodules
   add operational drag for a laptop-scale scaffold. If the upstream
   function changes, update both sides together. (`extract_igsn` from the
   same source is no longer needed here — `/aimdl/datafiles` filters
   server-side on `meta.igsn` existence, so every returned item already
   carries it.)

3. **`ConfigurableResource` (`GirderResource`) for the Girder client;
   `EnvVar` for credentials.** Resource construction is centralized; the API
   key never appears in run config or logs.

4. **`RunConfig` (`EnrichConfig`) for all per-run knobs.** Parent id,
   parent type, dry-run, overwrite policy, limits — all live in run
   config so each invocation is reproducible from the YAML.

5. **Idempotency for `experiment_date`.** Overwritten only if
   `overwrite_existing=True`. The `/aimdl/datafiles` projection does not
   include `meta.experiment_date`, so honoring `overwrite_existing=False`
   requires a per-item `GET /item/{id}` to check before writing. This
   doubles the read cost during backfill — acceptable for a one-shot
   tool, and skipped entirely when `overwrite_existing=True`.

6. **Defaults bias to safety.** `dry_run=True`, `preflight_only=False`,
   `overwrite_existing=False`. The first time you materialize without
   reading the YAML, nothing gets written.

7. **Pagination is forced to `sort=_id`.** The endpoint defaults to
   `sort=lowerName`, but pdv_trace items frequently share names (an item
   and its `copyOfItem` carry the same `name`), and MongoDB
   sort+skip+limit is unstable under ties — pages can overlap or skip
   silently. Sorting by the unique `_id` field gives stable pagination.
   Filed upstream as a footgun in `Xarthisius/girder-jsonforms`.

## Resolved by reading the source

- **Filter syntax.** `/aimdl/datafiles` accepts `dataType` (camelCase)
  as the only filter param. The earlier `partition_filter_mode` enum
  hedging snake-case / `query={…}` / `filter={…}` was speculative; the
  `aimdl.py` source resolves it to a single answer.
- **Pagination shape.** Standard Girder `pagingParams`: `limit` (≤100),
  `offset`, `sort`. Page-2-first-record vs page-1-first-record was
  empirically confirmed to advance, after fixing the sort.
- **Parent type.** `baseParentType` accepts `"collection"` or `"user"`
  only — not `"folder"`. With both `baseParentId` and `baseParentType`
  unset, the endpoint defaults to the entire AIMDL collection
  (`665de536bcc722774ce53754`).

## Hard footguns

- **Do NOT add `from __future__ import annotations` to
  `app/definitions.py`.** Dagster inspects parameter type hints at
  runtime to inject resources (`girder: ResourceParam[GirderClient]`).
  PEP 563 string annotations defer evaluation and break that injection —
  silently, with a confusing error.
- **Do NOT run a non-dry-run write (`run_config.first_write.yaml`)
  without explicit user confirmation in chat.** Preflight + dry-run
  exist precisely so the first real write is a deliberate decision.
- **Do NOT remove `sort=_id` from `build_datafiles_params`.** Without
  it, the default `sort=lowerName` plus duplicate-name items will
  silently skip records during pagination (some items will never be
  enriched).
