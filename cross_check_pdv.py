"""Independent cross-check of the pdv_trace inventory in Girder.

This script intentionally shares no code with the Dagster asset or with
verify_experiment_dates.py — in particular it does not import parse_date
from app/. It exists to validate, from the outside, the numbers the
asset and the existing verify script produce.

What it does:

  1. Counts cross-check (idea 1)
     - GET /aimdl/count for the configured scope, no dataType filter
       (matches the curl `/aimdl/count?baseParentId=…&baseParentType=…`).
     - GET /aimdl/count for the same scope with dataType=pdv_trace.
     - Paginate /aimdl/datafiles for dataType=pdv_trace and count rows.
     - Print all three side-by-side. Any gap between the dataType
       count and the /aimdl/datafiles count is the server-side
       meta.igsn filter (or anything else /aimdl/datafiles applies on
       top of dataType).

  2. Manual sample (idea 3)
     - Random-shuffle the items returned by /aimdl/datafiles.
     - Fetch full meta one at a time until ~25 items with
       meta.experiment_date set and ~25 without are collected.
     - Print _id, name, and full meta for each, so the user can
       eyeball them.

Reads GIRDER_API_URL, GIRDER_API_KEY, BASE_PARENT_ID, BASE_PARENT_TYPE
from the environment (same convention as verify_experiment_dates.py).

Exit codes:
  0 — ran to completion
  2 — env vars missing or the endpoint could not be reached
"""

import logging
import os
import random
import sys

import girder_client


LIMIT = 100
MAX_PAGES = 1000
SAMPLE_PER_BUCKET = 25
MAX_FETCH_TO_FILL = 1500  # safety cap on per-item GETs while sampling


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

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    log = logging.getLogger("cross_check")

    client = girder_client.GirderClient(apiUrl=api_url)
    client.authenticate(apiKey=api_key)

    scope_params: dict = {}
    if parent_id:
        scope_params["baseParentId"] = parent_id
    if parent_type:
        scope_params["baseParentType"] = parent_type

    # ----- Idea 1: counts from an independent endpoint -----
    # /aimdl/count returns a {dataType: count} breakdown for the whole scope
    # and ignores the dataType query parameter — so one call is enough; pull
    # the pdv_trace count out of the dict.
    log.info("GET /aimdl/count")
    try:
        count_breakdown = client.get("aimdl/count", parameters=scope_params)
    except Exception as exc:
        print(f"ERROR: GET /aimdl/count failed: {exc}", file=sys.stderr)
        return 2

    if not isinstance(count_breakdown, dict):
        print(
            f"ERROR: /aimdl/count returned {type(count_breakdown).__name__}, "
            f"expected dict: {count_breakdown!r}",
            file=sys.stderr,
        )
        return 2

    total_in_scope = sum(int(v) for v in count_breakdown.values())
    pdv_count = int(count_breakdown.get("pdv_trace", 0))

    log.info("Walking /aimdl/datafiles (dataType=pdv_trace)")
    items: list[dict] = []
    base_params = {
        **scope_params,
        "dataType": "pdv_trace",
        "sort": "_id",
        "limit": LIMIT,
    }
    for page_index in range(MAX_PAGES):
        params = {**base_params, "offset": page_index * LIMIT}
        try:
            page = client.get("aimdl/datafiles", parameters=params)
        except Exception as exc:
            print(f"ERROR: GET /aimdl/datafiles failed: {exc}", file=sys.stderr)
            return 2
        if not isinstance(page, list) or not page:
            break
        items.extend(page)
        if len(page) < LIMIT:
            break

    datafiles_count = len(items)

    print()
    print("=" * 72)
    print("COUNTS")
    print(f"  /aimdl/count breakdown (total {total_in_scope}):")
    for k, v in sorted(count_breakdown.items()):
        print(f"    {k}: {v}")
    print(f"  /aimdl/count pdv_trace:                {pdv_count}")
    print(f"  /aimdl/datafiles (dataType=pdv_trace): {datafiles_count}")
    gap = pdv_count - datafiles_count
    if gap == 0:
        print("  GAP: 0 — /aimdl/datafiles returned every pdv_trace item.")
    else:
        print(
            f"  GAP: {gap} — items counted as pdv_trace but not returned "
            f"by /aimdl/datafiles (likely the server-side meta.igsn filter)."
        )
    print("=" * 72)

    # ----- Idea 3: manual random sample, both buckets -----
    if not items:
        print("\nNo items to sample.")
        return 0

    random.shuffle(items)
    has_date: list[dict] = []
    no_date: list[dict] = []
    fetched = 0
    fetch_errors = 0

    for raw in items:
        if (
            len(has_date) >= SAMPLE_PER_BUCKET
            and len(no_date) >= SAMPLE_PER_BUCKET
        ):
            break
        if fetched >= MAX_FETCH_TO_FILL:
            break
        item_id = raw.get("_id")
        if not isinstance(item_id, str):
            continue
        fetched += 1
        try:
            item = client.get(f"item/{item_id}")
        except Exception as exc:
            log.error(f"failed to GET item/{item_id}: {exc}")
            fetch_errors += 1
            continue
        meta = item.get("meta") or {}
        record = {
            "id": item_id,
            "name": item.get("name") or raw.get("name") or "",
            "meta": meta,
        }
        if meta.get("experiment_date"):
            if len(has_date) < SAMPLE_PER_BUCKET:
                has_date.append(record)
        else:
            if len(no_date) < SAMPLE_PER_BUCKET:
                no_date.append(record)

    print()
    print(
        f"Random sample: fetched={fetched}, fetch_errors={fetch_errors}, "
        f"has_date_collected={len(has_date)}, no_date_collected={len(no_date)}"
    )

    def dump(label: str, samples: list[dict]) -> None:
        print()
        print("-" * 72)
        print(f"{label} ({len(samples)} samples)")
        print("-" * 72)
        if not samples:
            print("  (none)")
            return
        for r in samples:
            print(f"  _id:  {r['id']}")
            print(f"  name: {r['name']}")
            print("  meta:")
            for k, v in sorted(r["meta"].items()):
                print(f"    {k}: {v}")
            print()

    dump("HAS meta.experiment_date", has_date)
    dump("MISSING meta.experiment_date", no_date)

    return 0


if __name__ == "__main__":
    sys.exit(main())
