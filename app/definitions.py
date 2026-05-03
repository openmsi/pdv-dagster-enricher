# NOTE: do NOT add `from __future__ import annotations` to this module.
# Dagster inspects function parameter type hints at runtime to wire up
# resource injection (e.g. `girder: ResourceParam[GirderClient]`). PEP 563
# string annotations defer evaluation, which breaks that lookup.

import re
from typing import Any, Literal, Optional

import dagster as dg
import dateutil.parser
import girder_client
from dagster import EnvVar, ResourceParam
from girder_client import GirderClient


# ---------------------------------------------------------------------------
# Verbatim copy of parse_date() from
#   xarthisius/girder-consumers/common/base.py
# Source path:
#   /Users/elbert/Documents/GitHub/xarthisius/girder-consumers/common/base.py
# Kept verbatim (modulo this attribution) so behavior matches the upstream
# helix uploader exactly. Update both sides together if either changes.
# ---------------------------------------------------------------------------

_date_time_pattern = re.compile(
    r"(?<![0-9])(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})(?![0-9])"
)


def parse_date(name, logger):
    """Parse experiment_date from a filename or folder name.

    Matches the first occurrence of YYYY-MM-DD_HH-MM-SS anywhere in *name*,
    regardless of how many underscore-delimited segments surround it.
    Examples that are handled correctly:
      - ``IGSN_a_b_c_2026-04-15_17-10-04``
      - ``JHAMAL00004-01_abc_1_894_2026-04-15_17-10-04_shot10_ch1.csv``
    """
    metadata = {}
    if m := _date_time_pattern.search(name):
        try:
            date, time = m.group(1), m.group(2)
            metadata["experiment_date"] = dateutil.parser.parse(
                f"{date} {time.replace('-', ':')}+00:00"
            ).isoformat()
        except Exception as exc:
            msg = f"Error parsing date and time from '{name}': {exc}"
            logger.error(msg, exc_info=exc)
    return metadata


# ---------------------------------------------------------------------------
# Resource
# ---------------------------------------------------------------------------


class GirderResource(dg.ConfigurableResource):
    api_url: str
    api_key: str

    def create_resource(self, context) -> GirderClient:
        client = girder_client.GirderClient(apiUrl=self.api_url)
        client.authenticate(apiKey=self.api_key)
        return client


# ---------------------------------------------------------------------------
# Run config
# ---------------------------------------------------------------------------


# Server-side limit cap; girder-jsonforms aimdl.py:254 rejects > 100.
MAX_LIMIT = 100


class EnrichConfig(dg.Config):
    base_parent_id: str
    base_parent_type: Literal["collection", "user"] = "collection"
    dry_run: bool = True
    preflight_only: bool = False
    overwrite_existing: bool = False
    limit: int = MAX_LIMIT
    max_pages: int = 100


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def build_datafiles_params(config: EnrichConfig, *, offset: int) -> dict[str, Any]:
    """Build query params for /aimdl/datafiles.

    sort=_id is forced. The endpoint's default sort is `lowerName`, but
    pdv_trace items have many duplicate names (an item and its
    `copyOfItem` share a `name`) and MongoDB sort+skip+limit is not
    stable under ties — the same _id can appear on consecutive pages or
    be skipped entirely. Sorting by the unique _id makes pagination
    stable.
    """
    return {
        "baseParentId": config.base_parent_id,
        "baseParentType": config.base_parent_type,
        "dataType": "pdv_trace",
        "sort": "_id",
        "offset": offset,
        "limit": min(config.limit, MAX_LIMIT),
    }


def normalize_item(raw: dict[str, Any]) -> Optional[tuple[str, str]]:
    """Return (item_id, name) from a /aimdl/datafiles record, or None.

    The endpoint filters by meta.igsn and meta.data_type=pdv_trace
    server-side (girder-jsonforms aimdl.py:259-262), so neither needs
    re-checking here. Only _id (to address the item) and name (to parse
    the date from) are required downstream.
    """
    item_id = raw.get("_id")
    name = raw.get("name")
    if isinstance(item_id, str) and isinstance(name, str):
        return item_id, name
    return None


def fetch_existing_experiment_date(
    client: GirderClient, item_id: str
) -> Optional[str]:
    """Return meta.experiment_date for *item_id*, or None if absent.

    /aimdl/datafiles does not project meta.experiment_date, so when
    overwrite_existing=False we have to read the item directly to honor
    the policy. Doubles the round-trip count during backfill — acceptable
    for a one-shot tool, and skipped when overwrite_existing=True.
    """
    item = client.get(f"item/{item_id}")
    return (item.get("meta") or {}).get("experiment_date")


def add_metadata_to_item(
    client: GirderClient, item_id: str, metadata: dict[str, Any]
) -> None:
    client.addMetadataToItem(item_id, metadata)


def _run_metadata(config: EnrichConfig, *, preflight_only: bool) -> dict[str, Any]:
    return {
        "base_parent_id": config.base_parent_id,
        "base_parent_type": config.base_parent_type,
        "dry_run": config.dry_run,
        "preflight_only": preflight_only,
        "overwrite_existing": config.overwrite_existing,
        "limit": min(config.limit, MAX_LIMIT),
    }


# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------


def run_preflight(
    context: dg.AssetExecutionContext,
    config: EnrichConfig,
    client: GirderClient,
) -> None:
    """Sanity-check /aimdl/datafiles before pagination starts.

    Confirms page 0 returns records with the expected shape, and — when
    page 0 is full — that offset actually advances under sort=_id.
    """
    log = context.log

    page0_params = build_datafiles_params(config, offset=0)
    log.info(f"[preflight] page 0 params: {page0_params}")
    page0 = client.get("aimdl/datafiles", parameters=page0_params)

    if not isinstance(page0, list):
        raise RuntimeError(
            f"Preflight failed: /aimdl/datafiles returned {type(page0).__name__}, "
            f"expected list. Body: {page0!r}"
        )
    if not page0:
        raise RuntimeError(
            "Preflight failed: /aimdl/datafiles returned zero records for "
            f"baseParentId={config.base_parent_id} "
            f"baseParentType={config.base_parent_type}. "
            "Check parent id, parent type, and that the parent contains "
            "pdv_trace items with meta.igsn set."
        )

    sample = page0[:5]
    missing_id = sum(1 for r in sample if not r.get("_id"))
    missing_name = sum(1 for r in sample if not r.get("name"))

    log.info(
        f"[preflight] page 0: {len(page0)} record(s); "
        f"missing_id={missing_id} missing_name={missing_name}"
    )
    log.info(f"[preflight] first record: {page0[0]!r}")

    if missing_id or missing_name:
        raise RuntimeError(
            "Preflight failed: records missing _id or name in sample of 5. "
            f"missing_id={missing_id} missing_name={missing_name}"
        )

    limit = min(config.limit, MAX_LIMIT)
    if len(page0) >= limit:
        page1_params = build_datafiles_params(config, offset=limit)
        log.info(f"[preflight] page 1 params (offset check): {page1_params}")
        page1 = client.get("aimdl/datafiles", parameters=page1_params)
        if (
            isinstance(page1, list)
            and page1
            and page1[0].get("_id") == page0[0].get("_id")
        ):
            raise RuntimeError(
                "Preflight failed: page 1 first _id matches page 0 first _id "
                "even with sort=_id. Pagination would loop forever."
            )


# ---------------------------------------------------------------------------
# Asset
# ---------------------------------------------------------------------------


@dg.asset(group_name="helix_enrichment", compute_kind="python")
def enrich_pdv_trace_experiment_dates(
    context: dg.AssetExecutionContext,
    config: EnrichConfig,
    girder: ResourceParam[GirderClient],
) -> None:
    log = context.log

    run_preflight(context, config, girder)

    counters = {
        "pages": 0,
        "scanned": 0,
        "updated": 0,
        "would_update": 0,
        "skipped_no_date": 0,
        "skipped_existing_date": 0,
        "skipped_malformed": 0,
        "errors": 0,
    }

    if config.preflight_only:
        log.info("[asset] preflight_only=True, returning without paginating.")
        context.add_output_metadata(
            {**counters, **_run_metadata(config, preflight_only=True)}
        )
        return

    limit = min(config.limit, MAX_LIMIT)

    for page_index in range(config.max_pages):
        offset = page_index * limit
        params = build_datafiles_params(config, offset=offset)
        log.info(f"[page {page_index}] params: {params}")
        page = girder.get("aimdl/datafiles", parameters=params)

        if not isinstance(page, list):
            log.error(
                f"[page {page_index}] unexpected response type: "
                f"{type(page).__name__}; stopping pagination."
            )
            counters["errors"] += 1
            break

        if not page:
            log.info(f"[page {page_index}] empty page, stopping pagination.")
            break

        page_counters = {
            "scanned": 0,
            "updated": 0,
            "would_update": 0,
            "skipped_no_date": 0,
            "skipped_existing_date": 0,
            "skipped_malformed": 0,
            "errors": 0,
        }

        for raw in page:
            page_counters["scanned"] += 1
            try:
                norm = normalize_item(raw)
                if norm is None:
                    log.warning(
                        f"[page {page_index}] malformed record, skipping: {raw!r}"
                    )
                    page_counters["skipped_malformed"] += 1
                    continue
                item_id, name = norm

                parsed = parse_date(name, log)
                if "experiment_date" not in parsed:
                    page_counters["skipped_no_date"] += 1
                    continue

                if not config.overwrite_existing:
                    existing = fetch_existing_experiment_date(girder, item_id)
                    if existing:
                        page_counters["skipped_existing_date"] += 1
                        continue

                update = {"experiment_date": parsed["experiment_date"]}

                if config.dry_run:
                    page_counters["would_update"] += 1
                    log.info(
                        f"[page {page_index}] DRY RUN would update "
                        f"item_id={item_id} name={name!r} with {update}"
                    )
                else:
                    add_metadata_to_item(girder, item_id, update)
                    page_counters["updated"] += 1

            except Exception as exc:
                page_counters["errors"] += 1
                log.error(
                    f"[page {page_index}] error processing record {raw!r}: {exc}",
                    exc_info=True,
                )

        for key, value in page_counters.items():
            counters[key] += value
        counters["pages"] += 1

        log.info(
            f"[page {page_index} summary] offset={offset} "
            f"scanned={page_counters['scanned']} "
            f"updated={page_counters['updated']} "
            f"would_update={page_counters['would_update']} "
            f"skipped_no_date={page_counters['skipped_no_date']} "
            f"skipped_existing_date={page_counters['skipped_existing_date']} "
            f"skipped_malformed={page_counters['skipped_malformed']} "
            f"errors={page_counters['errors']}"
        )

        if len(page) < limit:
            log.info(
                f"[page {page_index}] short page ({len(page)} < {limit}), "
                "stopping pagination."
            )
            break

    context.add_output_metadata(
        {**counters, **_run_metadata(config, preflight_only=False)}
    )


# ---------------------------------------------------------------------------
# Job + Definitions
# ---------------------------------------------------------------------------


enrich_pdv_trace_job = dg.define_asset_job(
    name="enrich_pdv_trace_job",
    selection=[enrich_pdv_trace_experiment_dates],
)


defs = dg.Definitions(
    assets=[enrich_pdv_trace_experiment_dates],
    jobs=[enrich_pdv_trace_job],
    resources={
        "girder": GirderResource(
            api_url=EnvVar("GIRDER_API_URL"),
            api_key=EnvVar("GIRDER_API_KEY"),
        ),
    },
)
