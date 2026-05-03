# Decisions

Locked design choices for this scaffold. Revisit only with explicit reason.

## Locked decisions

1. **Single `@asset` with rich output metadata, not a multi-asset graph.** The
   work is one logical unit (paginate, normalize, enrich); splitting into a
   graph adds materialization plumbing and IO managers without buying
   anything observable that we don't already get from per-page log lines and
   `context.add_output_metadata`.

2. **`extract_igsn` and `parse_date` are copied verbatim from
   `xarthisius/girder-consumers/common/base.py`** with an attribution comment.
   Not pip-installed and not pulled in as a git submodule, because that repo
   is not packaged and submodules add operational drag for a laptop-scale
   scaffold. If either function changes upstream, update both sides together.

3. **`ConfigurableResource` (`GirderResource`) for the Girder client;
   `EnvVar` for credentials.** Resource construction is centralized; the API
   key never appears in run config or logs.

4. **`RunConfig` (`EnrichConfig`) for all per-run knobs.** Parent id, dry-run,
   filter mode, pagination mode, etc. live in run config so each invocation
   is reproducible from the YAML.

5. **Idempotency.** `experiment_date` is overwritten only if
   `overwrite_existing=True`. `igsn` is fill-if-absent — identity-like
   metadata, never clobbered.

6. **Defaults bias to safety.** `dry_run=True`, `preflight_only=False`,
   `overwrite_existing=False`. The first time you materialize without
   reading the YAML, nothing gets written.

## Open questions (resolved by probe + preflight)

- **`/aimdl/partition` filter syntax** — does it accept a `data_type`
  query param, a JSON-encoded `query` (`{"meta.data_type": "pdv_trace"}`),
  or a JSON-encoded `filter` (`{"data_type": "pdv_trace"}`)? `probe_partition.py`
  tries all three. The follow-up commit collapses `partition_filter_mode`
  to the winner.
- **Pagination shape** — does the endpoint honor `offset`/`limit` with
  stable ordering, or does it return a single page? Preflight detects
  `offset` being silently ignored by comparing the first record across
  pages 0 and 1. The follow-up commit collapses `pagination_mode` to the
  observed shape.

## Hard footguns

- **Do NOT add `from __future__ import annotations` to `app/definitions.py`.**
  Dagster inspects parameter type hints at runtime to inject resources
  (`girder: ResourceParam[GirderClient]`). PEP 563 string annotations defer
  evaluation and break that injection — silently, with a confusing error.
- **Do NOT run a non-dry-run write (`run_config.first_write.yaml`) without
  explicit user confirmation in chat.** Preflight + dry-run exist precisely
  so the first real write is a deliberate decision.
