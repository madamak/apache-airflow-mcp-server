from __future__ import annotations

from ..client_factory import get_client_factory
from ..observability import operation_logger
from ..url_utils import resolve_and_validate
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
        resp = api.get_dataset_events(limit=limit_int, uri=dataset_uri_value)
        events = [
            e.to_dict() if hasattr(e, "to_dict") else e
            for e in getattr(resp, "dataset_events", []) or []
        ]
        payload = {
            "events": events,
            "count": getattr(resp, "total_entries", len(events)),
        }
        return op.success(payload)
