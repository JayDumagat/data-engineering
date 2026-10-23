"""Dataset-aware cleanup for generic PSE EDGE HTML table rows."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
import re


# These are informational/form pages, not business datasets. Keep extracted
# rows in the lake for traceability, but route them away from the warehouse.
PRESENTATION_TABLE_DATASETS = frozenset(
    {"advanced_search_page", "contact_us", "etf_search_page"}
)

_EMPTY_ROW_VALUES = frozenset(
    {
        "",
        "-",
        "--",
        "—",
        "–",
        "n/a",
        "na",
        "not available",
        "tba",
        "no data",
        "no data available",
        "no record",
        "no records",
        "no records found",
        "no result",
        "no results",
        "no results found",
        "no matching records",
        "null",
        "none",
    }
)
_NUMERIC_OR_CHANGE = re.compile(r"^[+-]?(?:\d[\d,]*(?:\.\d+)?%?|\.\d+%?)(?:[▲▼↑↓])?$")


def _text(value: Any) -> str:
    return " ".join(str(value if value is not None else "").split()).strip()


def _row_values(payload: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    columns = payload.get("columns")
    if isinstance(columns, Mapping):
        values.extend(_text(value) for value in columns.values())
    cells = payload.get("cells")
    if isinstance(cells, (list, tuple)):
        values.extend(_text(value) for value in cells)
    return [value for value in values if value]


def _is_empty_state(payload: Mapping[str, Any]) -> bool:
    values = _row_values(payload)
    if not values:
        return True
    normalized = [value.casefold().rstrip(".!:").strip() for value in values]
    return all(value in _EMPTY_ROW_VALUES for value in normalized)


def _display_title(dataset: str, payload: Mapping[str, Any]) -> str | None:
    columns = payload.get("columns")
    if not isinstance(columns, Mapping):
        return None

    normalized = {
        re.sub(r"[^a-z0-9]", "", str(key).casefold()): _text(value)
        for key, value in columns.items()
    }
    preferred = (
        "companyname",
        "company",
        "issuer",
        "security",
        "symbol",
        "ticker",
        "index",
        "statisticname",
        "metric",
        "event",
        "reporttitle",
        "reporttype",
        "subject",
        "recorddate",
        "exdate",
        "date",
        "name",
        "title",
    )
    for key in preferred:
        candidate = normalized.get(key)
        if candidate and not _is_placeholder(candidate):
            return candidate

    for value in normalized.values():
        if value and not _is_placeholder(value) and not _NUMERIC_OR_CHANGE.fullmatch(value):
            return value

    if dataset == "index_summary":
        return "Index summary metric"
    return None


def _is_placeholder(value: str) -> bool:
    normalized = value.casefold().rstrip(".!:").strip()
    return normalized in _EMPTY_ROW_VALUES


def prepare_record(dataset: str, payload: Mapping[str, Any]) -> dict[str, Any] | None:
    """Drop empty-state rows and keep page chrome out of business sinks."""
    prepared = dict(payload)
    if prepared.get("record_type") != "table_row":
        return prepared
    if _is_empty_state(prepared):
        return None

    if not _text(prepared.get("title")):
        title = _display_title(dataset, prepared)
        if title:
            prepared["title"] = title

    if dataset in PRESENTATION_TABLE_DATASETS:
        prepared["record_type"] = "source_table_row"
    return prepared
