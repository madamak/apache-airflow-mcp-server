from __future__ import annotations

from ..client_factory import get_client_factory
from ..errors import AirflowToolError
from ..observability import operation_logger
from ..url_utils import resolve_and_validate
from ..utils import json_safe_recursive as _json_safe
from ..validation import validate_dataset_uri
from ._common import _coerce_int

_factory = get_client_factory()


def dataset_events(
    instance: str | None = None,
    ui_url: str | None = None,
    dataset_uri: str | None = None,
    limit: int | float | str = 50,
) -> str:
    """List dataset events.

    Parameters
    - instance | ui_url: Target instance selection
    - dataset_uri: Required dataset URI
    - limit: Max results

    Returns
    - JSON: { "events": [object], "count": int } or { "error": "..." }
    """
    with operation_logger(
        "airflow_dataset_events", instance=instance, dataset_uri=dataset_uri, limit=limit
    ) as op:
        dataset_uri_value = validate_dataset_uri(dataset_uri)
        resolved = resolve_and_validate(ui_url=ui_url, instance=instance)
        op.update_context(instance=resolved.instance, dataset_uri=dataset_uri_value)
        # Coerce limit
        limit_int = _coerce_int(limit)
        if limit_int is None:
            limit_int = 50
        if limit_int < 0:
            limit_int = 0

        api = _factory.get_dataset_events_api(resolved.instance)
        if _factory.get_api_family(resolved.instance) == "v2":
            # Airflow 3 renamed datasets to assets and filters events by asset_id,
            # so resolve the asset by URI first. uri_pattern is a substring match,
            # so paginate until the exact URI is found.
            asset_id = None
            page_size = 100
            max_pages = 50  # backstop against a pathological pattern match
            offset = 0
            for _ in range(max_pages):
                assets_resp = api.get_assets(
                    uri_pattern=dataset_uri_value, limit=page_size, offset=offset
                )
                assets = getattr(assets_resp, "assets", []) or []
                for asset in assets:
                    if getattr(asset, "uri", None) == dataset_uri_value:
                        asset_id = getattr(asset, "id", None)
                        break
                if asset_id is not None or len(assets) < page_size:
                    break
                offset += page_size
            if asset_id is None:
                raise AirflowToolError(
                    f"No asset found with uri '{dataset_uri_value}'",
                    code="NOT_FOUND",
                    context={"dataset_uri": dataset_uri_value},
                )
            resp = api.get_asset_events(asset_id=asset_id, limit=limit_int)
            raw_events = getattr(resp, "asset_events", []) or []
        else:
            resp = api.get_dataset_events(limit=limit_int, uri=dataset_uri_value)
            raw_events = getattr(resp, "dataset_events", []) or []
        payload = {
            "events": _json_safe(raw_events),
            "count": getattr(resp, "total_entries", len(raw_events)),
        }
        return op.success(payload)
