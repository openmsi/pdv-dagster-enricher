"""Standalone probe for the AIMD-L /aimdl/datafiles endpoint.

Reads GIRDER_API_URL and GIRDER_API_KEY (required) plus BASE_PARENT_ID
and BASE_PARENT_TYPE (optional; if absent, the server defaults to the
whole AIMDL collection) from the environment. Calls /aimdl/datafiles for
dataType=pdv_trace at offset=0 and offset=LIMIT (limit=5 each) to confirm
response shape and that offset actually advances pagination, then GETs
the first item via /item/{id} to show its full meta — so we can see
whether the existing population already carries meta.experiment_date.

The endpoint accepts baseParentType ∈ {"collection", "user"} only;
"folder" is not supported. Param names are camelCase: baseParentId,
baseParentType, dataType.

This script intentionally does not depend on Dagster.
"""

import json
import os
import sys

import girder_client


PREVIEW_CHARS = 4000
LIMIT = 5


def _print_preview(response):
    try:
        preview = json.dumps(response, indent=2, default=str)
    except Exception:
        preview = repr(response)
    if len(preview) > PREVIEW_CHARS:
        preview = preview[:PREVIEW_CHARS] + f"\n... [truncated at {PREVIEW_CHARS} chars]"
    print(preview)


def main() -> int:
    api_url = os.getenv("GIRDER_API_URL")
    api_key = os.getenv("GIRDER_API_KEY")
    parent_id = os.getenv("BASE_PARENT_ID")
    parent_type = os.getenv("BASE_PARENT_TYPE")

    missing = [
        name
        for name, value in [
            ("GIRDER_API_URL", api_url),
            ("GIRDER_API_KEY", api_key),
        ]
        if not value
    ]
    if missing:
        print(f"ERROR: missing env var(s): {', '.join(missing)}", file=sys.stderr)
        return 2

    client = girder_client.GirderClient(apiUrl=api_url)
    client.authenticate(apiKey=api_key)

    base_params: dict = {
        "dataType": "pdv_trace",
        "limit": LIMIT,
    }
    if parent_id:
        base_params["baseParentId"] = parent_id
    if parent_type:
        base_params["baseParentType"] = parent_type

    pages = {
        "offset_0": {**base_params, "offset": 0},
        "offset_5": {**base_params, "offset": LIMIT},
    }

    page_results: dict = {}
    for label, params in pages.items():
        print("=" * 72)
        print(f"CALL: GET /aimdl/datafiles  ({label})")
        print(f"PARAMS: {params}")
        try:
            response = client.get("aimdl/datafiles", parameters=params)
        except Exception as exc:
            print(f"ERROR: {type(exc).__name__}: {exc}")
            page_results[label] = None
            continue
        _print_preview(response)
        page_results[label] = response

    # Per-item probe: fetch the first item from page 0 to inspect full meta.
    # This is what tells us whether the existing population already has
    # meta.experiment_date (and so how much actual backfill work there is).
    page0 = page_results.get("offset_0")
    first_item = (
        page0[0]
        if isinstance(page0, list) and page0 and isinstance(page0[0], dict)
        else None
    )
    if first_item and first_item.get("_id"):
        first_id = first_item["_id"]
        print("=" * 72)
        print(f"CALL: GET /item/{first_id}")
        try:
            item = client.get(f"item/{first_id}")
        except Exception as exc:
            print(f"ERROR: {type(exc).__name__}: {exc}")
        else:
            _print_preview(item)
            meta = item.get("meta", {}) if isinstance(item, dict) else {}
            print()
            print(f"  meta keys: {sorted(meta.keys())}")
            print(f"  meta.data_type: {meta.get('data_type')!r}")
            print(f"  meta.igsn present: {'igsn' in meta}")
            print(f"  meta.experiment_date present: {'experiment_date' in meta}")
            if "experiment_date" in meta:
                print(f"  meta.experiment_date value: {meta['experiment_date']!r}")

    print("=" * 72)
    print("SUMMARY:")
    for label, recs in page_results.items():
        if not isinstance(recs, list):
            print(f"  {label}: ERROR or unexpected response shape")
            continue
        first_id = recs[0].get("_id") if recs and isinstance(recs[0], dict) else None
        print(f"  {label}: {len(recs)} record(s); first _id = {first_id}")

    page1 = page_results.get("offset_5")
    if isinstance(page0, list) and isinstance(page1, list) and page0 and page1:
        f0 = page0[0].get("_id") if isinstance(page0[0], dict) else None
        f1 = page1[0].get("_id") if isinstance(page1[0], dict) else None
        if f0 and f1:
            print(f"  offset advances: {'NO — offset ignored' if f0 == f1 else 'YES'}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
