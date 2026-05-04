"""Verify meta.experiment_date on enriched pdv_trace items.

Pulls every pdv_trace item under the configured scope via
/aimdl/datafiles, fetches each item's full meta (since the endpoint does
not project meta.experiment_date), and compares the stored value
against what parse_date() — the same function used by the asset and by
the upstream helix uploader — would produce from the item's name today.

Reads GIRDER_API_URL, GIRDER_API_KEY, BASE_PARENT_ID, and
BASE_PARENT_TYPE from the environment (same convention as
probe_partition.py). BASE_PARENT_TYPE must be "collection" or "user";
"folder" is not accepted by /aimdl/datafiles. If both BASE_PARENT_*
are unset, the endpoint defaults to the entire AIMDL collection.

Exit codes:
  0 — every item with a parseable filename has a matching stored date
  1 — at least one mismatch (stored != recomputed) was found
  2 — env vars missing or the endpoint could not be reached

This script intentionally does not depend on Dagster.
"""

import logging
import os
import sys

import girder_client

# Single source of truth — same parse_date the asset uses.
from app.definitions import parse_date


LIMIT = 100
MAX_PAGES = 1000  # safety cap for the pagination loop


class _LogShim:
    """Adapter so parse_date can use a stdlib logger.

    parse_date calls logger.error(msg, exc_info=exc) on failure. The
    stdlib Logger supports that signature, so this is a thin pass-through
    that exists mostly to make the dependency explicit.
    """

    def __init__(self, name: str = "verify"):
        self._log = logging.getLogger(name)

    def error(self, msg, exc_info=None):
        self._log.error(msg, exc_info=exc_info)


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
    log = logging.getLogger("verify")
    parse_log = _LogShim()

    client = girder_client.GirderClient(apiUrl=api_url)
    client.authenticate(apiKey=api_key)

    base_params: dict = {
        "dataType": "pdv_trace",
        "sort": "_id",
        "limit": LIMIT,
    }
    if parent_id:
        base_params["baseParentId"] = parent_id
    if parent_type:
        base_params["baseParentType"] = parent_type

    counts = {
        "scanned": 0,
        "match": 0,
        "mismatch": 0,
        "missing_date": 0,
        "unparseable_with_stored_date": 0,
        "unparseable_no_stored_date": 0,
        "fetch_errors": 0,
    }
    mismatches: list[dict] = []
    missing_date: list[dict] = []
    unparseable_with_stored: list[dict] = []

    for page_index in range(MAX_PAGES):
        offset = page_index * LIMIT
        params = {**base_params, "offset": offset}
        log.info(f"[page {page_index}] fetching offset={offset}")
        try:
            page = client.get("aimdl/datafiles", parameters=params)
        except Exception as exc:
            print(f"ERROR: GET /aimdl/datafiles failed: {exc}", file=sys.stderr)
            return 2
        if not isinstance(page, list) or not page:
            break

        for raw in page:
            counts["scanned"] += 1
            item_id = raw.get("_id")
            name = raw.get("name") or ""
            if not isinstance(item_id, str):
                continue

            recomputed = parse_date(name, parse_log).get("experiment_date")

            try:
                item = client.get(f"item/{item_id}")
            except Exception as exc:
                log.error(f"failed to GET item/{item_id}: {exc}")
                counts["fetch_errors"] += 1
                continue
            stored = (item.get("meta") or {}).get("experiment_date")

            if recomputed is None and stored is None:
                counts["unparseable_no_stored_date"] += 1
                continue
            if recomputed is None and stored is not None:
                counts["unparseable_with_stored_date"] += 1
                unparseable_with_stored.append(
                    {"id": item_id, "name": name, "stored": stored}
                )
                continue
            if stored is None:
                counts["missing_date"] += 1
                missing_date.append(
                    {"id": item_id, "name": name, "expected": recomputed}
                )
                continue
            if stored == recomputed:
                counts["match"] += 1
            else:
                counts["mismatch"] += 1
                mismatches.append(
                    {
                        "id": item_id,
                        "name": name,
                        "stored": stored,
                        "recomputed": recomputed,
                    }
                )

        if len(page) < LIMIT:
            break

    print()
    print("=" * 72)
    print("SUMMARY")
    for key, value in counts.items():
        print(f"  {key}: {value}")
    print("=" * 72)

    if missing_date:
        print(
            f"\nITEMS MISSING experiment_date "
            f"(filename parses, but field is absent — backfill candidates): "
            f"{len(missing_date)}"
        )
        for r in missing_date[:20]:
            print(f"  {r['id']}  expected={r['expected']}  name={r['name']}")
        if len(missing_date) > 20:
            print(f"  ... ({len(missing_date) - 20} more)")

    if unparseable_with_stored:
        print(
            f"\nITEMS WITH STORED DATE BUT UNPARSEABLE FILENAME: "
            f"{len(unparseable_with_stored)}"
        )
        print(
            "  These were either set by hand or by an older parse_date "
            "we cannot reproduce — inspect manually."
        )
        for r in unparseable_with_stored[:20]:
            print(f"  {r['id']}  stored={r['stored']}  name={r['name']}")
        if len(unparseable_with_stored) > 20:
            print(f"  ... ({len(unparseable_with_stored) - 20} more)")

    if mismatches:
        print(
            f"\nITEMS WITH MISMATCHED experiment_date "
            f"(stored != recomputed): {len(mismatches)}"
        )
        for r in mismatches[:50]:
            print(f"  {r['id']}")
            print(f"    name:       {r['name']}")
            print(f"    stored:     {r['stored']}")
            print(f"    recomputed: {r['recomputed']}")
        if len(mismatches) > 50:
            print(f"  ... ({len(mismatches) - 50} more)")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
