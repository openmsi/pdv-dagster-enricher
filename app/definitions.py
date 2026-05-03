# NOTE: do NOT add `from __future__ import annotations` to this module.
# Dagster inspects function parameter type hints at runtime to wire up
# resource injection (e.g. `girder: ResourceParam[GirderClient]`). PEP 563
# string annotations defer evaluation, which breaks that lookup.

import re
from dataclasses import dataclass, field
from typing import Any, Literal, Optional

import dagster as dg
import dateutil.parser
import girder_client
from dagster import EnvVar, ResourceParam
from girder_client import GirderClient


# ---------------------------------------------------------------------------
# Verbatim copy of extract_igsn() and parse_date() from
#   xarthisius/girder-consumers/common/base.py
# Source path:
#   /Users/elbert/Documents/GitHub/xarthisius/girder-consumers/common/base.py
# Kept verbatim (modulo this attribution) so behavior matches the upstream
# consumers exactly. Update both sides together if either changes.
# ---------------------------------------------------------------------------

igsn_pattern = re.compile(r"^[A-Z]{6}[0-9]{5}[A-Z0-9\-]*$", re.IGNORECASE)


def extract_igsn(filepath):
    """Find the first IGSN in any component of *filepath*.

    Tests the first underscore-delimited token of each path part, which
    handles both bare IGSN folder names and filenames/folder names like
    ``JHAMAL00004-01_abc_2026-04-15_...csv``.
    Returns the IGSN string (upper-cased) or ``None`` if not found.
    """
    for part in filepath.parts:
        candidate = part.split("_")[0]
        if igsn_pattern.match(candidate):
            return candidate.upper()
    return None


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


class EnrichConfig(dg.Config):
    base_parent_id: str
    base_parent_type: Literal["folder", "collection"] = "folder"
    dry_run: bool = True
    preflight_only: bool = False
    overwrite_existing: bool = False
    limit: int = 100
    max_pages: int = 100
    partition_filter_mode: Literal[
        "data_type_param", "query_json", "filter_json"
    ] = "data_type_param"
    pagination_mode: Literal["offset_limit", "single_page"] = "offset_limit"
    extra_partition_params: dict[str, Any] = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass
class PartitionRecord:
    item_id: Optional[str]
    name: Optional[str]
    path: Optional[str]
    metadata: dict[str, Any] = field(default_factory=dict)


def build_partition_params(config: EnrichConfig, *, offset: int, limit: int) -> dict[str, Any]:
    """Build query params for /aimdl/partition based on the chosen filter mode."""
    params: dict[str, Any] = {
        "parentId": config.base_parent_id,
        "parentType": config.base_parent_type,
    }

    if config.pagination_mode == "offset_limit":
        params["offset"] = offset
        params["limit"] = limit
    elif config.pagination_mode == "single_page":
        params["limit"] = limit
    else:
        raise ValueError(f"Unknown pagination_mode: {config.pagination_mode}")

    if config.partition_filter_mode == "data_type_param":
        params["data_type"] = "pdv_trace"
    elif config.partition_filter_mode == "query_json":
        import json
        params["query"] = json.dumps({"meta.data_type": "pdv_trace"})
    elif config.partition_filter_mode == "filter_json":
        import json
        params["filter"] = json.dumps({"data_type": "pdv_trace"})
    else:
        raise ValueError(
            f"Unknown partition_filter_mode: {config.partition_filter_mode}"
        )

    if config.extra_partition_params:
        params.update(config.extra_partition_params)

    return params


def get_partition_page(client: GirderClient, params: dict[str, Any]) -> Any:
    """Single GET against /aimdl/partition. Returns the raw decoded JSON."""
    return client.get("aimdl/partition", parameters=params)


def records_from_partition_response(response: Any) -> list[dict[str, Any]]:
    """Coerce the partition response into a flat list of record dicts.

    The endpoint contract is not yet pinned down. Tolerate:
      - a bare list of records
      - a dict with one of: results, files, items, data
    Anything else returns an empty list.
    """
    if isinstance(response, list):
        return response
    if isinstance(response, dict):
        for key in ("results", "files", "items", "data"):
            value = response.get(key)
            if isinstance(value, list):
                return value
    return []


def normalize_partition_record(raw: dict[str, Any]) -> PartitionRecord:
    """Best-effort mapping of a raw partition record to PartitionRecord.

    Defensive across likely shapes — partition records may inline metadata
    at the top level, nest it under ``meta``, or split path/name fields.
    """
    if not isinstance(raw, dict):
        return PartitionRecord(item_id=None, name=None, path=None, metadata={})

    item_id = (
        raw.get("itemId")
        or raw.get("item_id")
        or raw.get("_id")
        or raw.get("id")
    )

    name = raw.get("name") or raw.get("filename")
    path = raw.get("path") or raw.get("fullpath") or raw.get("relpath")

    metadata = {}
    if isinstance(raw.get("meta"), dict):
        metadata.update(raw["meta"])
    if isinstance(raw.get("metadata"), dict):
        metadata.update(raw["metadata"])
    # Some shapes inline data_type at the top level.
    if "data_type" in raw and "data_type" not in metadata:
        metadata["data_type"] = raw["data_type"]

    return PartitionRecord(
        item_id=item_id,
        name=name,
        path=path,
        metadata=metadata,
    )


def add_metadata_to_item(
    client: GirderClient, item_id: str, metadata: dict[str, Any]
) -> None:
    client.addMetadataToItem(item_id, metadata)


def record_identity(rec: PartitionRecord) -> tuple:
    """Stable identity tuple for detecting duplicate pages."""
    return (rec.item_id, rec.path, rec.name)


# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------


def run_preflight(
    context: dg.AssetExecutionContext,
    config: EnrichConfig,
    client: GirderClient,
) -> None:
    """Sanity-check the partition contract before we start writing.

    Always called before pagination. Raises on anything that would make the
    main loop unsafe (no records, missing item_ids, ignored offset).
    """
    log = context.log

    page0_params = build_partition_params(config, offset=0, limit=config.limit)
    log.info(f"[preflight] page 0 params: {page0_params}")
    page0_response = get_partition_page(client, page0_params)
    page0_raw = records_from_partition_response(page0_response)
    page0 = [normalize_partition_record(r) for r in page0_raw]

    sample_size = len(page0)
    missing_item_id = sum(1 for r in page0 if not r.item_id)
    missing_path = sum(1 for r in page0 if not r.path)
    missing_data_type = sum(1 for r in page0 if "data_type" not in r.metadata)
    explicit_pdv_trace = sum(
        1 for r in page0 if r.metadata.get("data_type") == "pdv_trace"
    )

    log.info(
        "[preflight] sample summary: "
        f"sample_size={sample_size} "
        f"missing_item_id={missing_item_id} "
        f"missing_path={missing_path} "
        f"missing_data_type={missing_data_type} "
        f"explicit_pdv_trace={explicit_pdv_trace}"
    )
    if page0:
        log.info(f"[preflight] first normalized record: {page0[0].__dict__}")

    if sample_size == 0:
        raise RuntimeError(
            "Preflight failed: /aimdl/partition returned zero records for "
            f"parent {config.base_parent_type}={config.base_parent_id}. "
            "Check parent id, filter mode, and that the parent contains pdv_trace items."
        )
    if missing_item_id:
        raise RuntimeError(
            f"Preflight failed: {missing_item_id}/{sample_size} records are "
            "missing item_id. Cannot enrich items we cannot address. "
            "Inspect the raw response shape and adjust normalize_partition_record."
        )

    if config.pagination_mode == "offset_limit" and sample_size == config.limit:
        page1_params = build_partition_params(
            config, offset=config.limit, limit=config.limit
        )
        log.info(f"[preflight] page 1 params (offset check): {page1_params}")
        page1_response = get_partition_page(client, page1_params)
        page1_raw = records_from_partition_response(page1_response)
        page1 = [normalize_partition_record(r) for r in page1_raw]
        if page1 and record_identity(page1[0]) == record_identity(page0[0]):
            raise RuntimeError(
                "Preflight failed: page 1 first record matches page 0 first record. "
                "The /aimdl/partition endpoint appears to ignore offset; pagination "
                "would loop forever. Switch pagination_mode or fix the call shape."
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
        "skipped_wrong_data_type": 0,
        "errors": 0,
    }

    if config.preflight_only:
        log.info("[asset] preflight_only=True, returning without paginating.")
        context.add_output_metadata(
            {
                **counters,
                "base_parent_id": config.base_parent_id,
                "base_parent_type": config.base_parent_type,
                "dry_run": config.dry_run,
                "preflight_only": True,
                "partition_filter_mode": config.partition_filter_mode,
                "pagination_mode": config.pagination_mode,
            }
        )
        return

    from pathlib import PurePosixPath

    for page_index in range(config.max_pages):
        offset = page_index * config.limit
        params = build_partition_params(config, offset=offset, limit=config.limit)
        log.info(f"[page {page_index}] params: {params}")
        response = get_partition_page(girder, params)
        raw_records = records_from_partition_response(response)

        if not raw_records:
            log.info(f"[page {page_index}] empty page, stopping pagination.")
            break

        page_counters = {
            "scanned": 0,
            "updated": 0,
            "would_update": 0,
            "skipped_no_date": 0,
            "skipped_existing_date": 0,
            "skipped_wrong_data_type": 0,
            "errors": 0,
        }

        for raw in raw_records:
            try:
                rec = normalize_partition_record(raw)
                page_counters["scanned"] += 1

                record_data_type = rec.metadata.get("data_type")
                # Trust the server when data_type is missing; only skip on
                # explicit mismatch.
                if record_data_type not in (None, "pdv_trace"):
                    page_counters["skipped_wrong_data_type"] += 1
                    continue

                if not rec.item_id:
                    log.warning(
                        f"[page {page_index}] record missing item_id, skipping: {raw!r}"
                    )
                    page_counters["errors"] += 1
                    continue

                name_for_date = rec.name or rec.path or ""
                parsed = parse_date(name_for_date, log)

                if "experiment_date" not in parsed:
                    page_counters["skipped_no_date"] += 1
                    continue

                existing = rec.metadata.get("experiment_date")
                if existing and not config.overwrite_existing:
                    page_counters["skipped_existing_date"] += 1
                    continue

                update: dict[str, Any] = {
                    "experiment_date": parsed["experiment_date"],
                }

                # IGSN is identity-like metadata, fill only when absent.
                if not rec.metadata.get("igsn") and rec.path:
                    igsn = extract_igsn(PurePosixPath(rec.path))
                    if igsn:
                        update["igsn"] = igsn

                if config.dry_run:
                    page_counters["would_update"] += 1
                    log.info(
                        f"[page {page_index}] DRY RUN would update "
                        f"item_id={rec.item_id} with {update}"
                    )
                else:
                    add_metadata_to_item(girder, rec.item_id, update)
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
            f"[page {page_index} summary] "
            f"offset={offset} "
            f"scanned={page_counters['scanned']} "
            f"updated={page_counters['updated']} "
            f"would_update={page_counters['would_update']} "
            f"skipped_no_date={page_counters['skipped_no_date']} "
            f"skipped_existing_date={page_counters['skipped_existing_date']} "
            f"skipped_wrong_data_type={page_counters['skipped_wrong_data_type']} "
            f"errors={page_counters['errors']}"
        )

        if config.pagination_mode == "single_page":
            break
        if len(raw_records) < config.limit:
            log.info(
                f"[page {page_index}] short page ({len(raw_records)} < {config.limit}), "
                "stopping pagination."
            )
            break

    context.add_output_metadata(
        {
            **counters,
            "base_parent_id": config.base_parent_id,
            "base_parent_type": config.base_parent_type,
            "dry_run": config.dry_run,
            "preflight_only": False,
            "partition_filter_mode": config.partition_filter_mode,
            "pagination_mode": config.pagination_mode,
        }
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
