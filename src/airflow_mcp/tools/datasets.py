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
            # so resolve the asset by URI first.
            assets_resp = api.get_assets(uri_pattern=dataset_uri_value, limit=100)
            asset_id = None
            for asset in getattr(assets_resp, "assets", []) or []:
                if getattr(asset, "uri", None) == dataset_uri_value:
                    asset_id = getattr(asset, "id", None)
                    break
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
        events = [e.to_dict() if hasattr(e, "to_dict") else e for e in raw_events]
        payload = {
            "events": _json_safe(events),
            "count": getattr(resp, "total_entries", len(events)),
        }
        return op.success(payload)
