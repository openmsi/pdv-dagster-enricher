"""Standalone probe for the AIMD-L /aimdl/partition endpoint.

Reads GIRDER_API_URL, GIRDER_API_KEY, BASE_PARENT_ID, and BASE_PARENT_TYPE
(default "folder") from the environment, authenticates a GirderClient, and
tries three filter modes against /aimdl/partition with limit=5, offset=0.
Prints a header per mode, the params used, and the first ~4000 chars of the
response body (or the exception). Ends with a summary line of which modes
returned a non-empty result list.

This script intentionally does not depend on Dagster.
"""

import json
import os
import sys

import girder_client


PREVIEW_CHARS = 4000
LIMIT = 5
OFFSET = 0


def records_from_response(response):
    if isinstance(response, list):
        return response
    if isinstance(response, dict):
        for key in ("results", "files", "items", "data"):
            value = response.get(key)
            if isinstance(value, list):
                return value
    return []


def main() -> int:
    api_url = os.getenv("GIRDER_API_URL")
    api_key = os.getenv("GIRDER_API_KEY")
    parent_id = os.getenv("BASE_PARENT_ID")
    parent_type = os.getenv("BASE_PARENT_TYPE", "folder")

    missing = [
        name
        for name, value in [
            ("GIRDER_API_URL", api_url),
            ("GIRDER_API_KEY", api_key),
            ("BASE_PARENT_ID", parent_id),
        ]
        if not value
    ]
    if missing:
        print(f"ERROR: missing env var(s): {', '.join(missing)}", file=sys.stderr)
        return 2

    client = girder_client.GirderClient(apiUrl=api_url)
    client.authenticate(apiKey=api_key)

    base_params = {
        "parentId": parent_id,
        "parentType": parent_type,
        "offset": OFFSET,
        "limit": LIMIT,
    }

    modes = {
        "data_type_param": {**base_params, "data_type": "pdv_trace"},
        "query_json": {
            **base_params,
            "query": json.dumps({"meta.data_type": "pdv_trace"}),
        },
        "filter_json": {
            **base_params,
            "filter": json.dumps({"data_type": "pdv_trace"}),
        },
    }

    results: dict[str, int] = {}

    for mode, params in modes.items():
        print("=" * 72)
        print(f"MODE: {mode}")
        print(f"PARAMS: {params}")
        try:
            response = client.get("aimdl/partition", parameters=params)
        except Exception as exc:
            print(f"ERROR: {type(exc).__name__}: {exc}")
            results[mode] = -1
            continue

        try:
            preview = json.dumps(response, indent=2, default=str)
        except Exception:
            preview = repr(response)

        if len(preview) > PREVIEW_CHARS:
            preview = preview[:PREVIEW_CHARS] + f"\n... [truncated at {PREVIEW_CHARS} chars]"
        print(preview)
        results[mode] = len(records_from_response(response))

    print("=" * 72)
    print("SUMMARY:")
    for mode, count in results.items():
        if count < 0:
            print(f"  {mode}: ERROR")
        else:
            print(f"  {mode}: {count} record(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
