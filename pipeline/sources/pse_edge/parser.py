"""Small dependency-free parsers for the public PSE EDGE HTML and RSS feeds."""

from __future__ import annotations

from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlparse
import re
import json
import xml.etree.ElementTree as ET


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[dict[str, Any]] = []
        self.links: list[dict[str, str]] = []
        self.text_parts: list[str] = []
        self._table_depth = 0
        self._table: dict[str, Any] | None = None
        self._row: dict[str, Any] | None = None
        self._cell: dict[str, Any] | None = None
        self._skip_depth = 0
        self._in_title = False
        self.title_parts: list[str] = []
        self._in_heading = False
        self._heading_parts: list[str] = []
        self._last_heading = ""
        self.form_fields: list[dict[str, Any]] = []
        self._select_name = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = {key.lower(): value or "" for key, value in attrs}
        if tag.lower() in {"script", "style", "noscript"}:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return
        if tag.lower() == "input" and attributes.get("name"):
            self.form_fields.append(
                {
                    "name": attributes["name"],
                    "value": attributes.get("value", ""),
                    "type": attributes.get("type", "text"),
                    "id": attributes.get("id", ""),
                }
            )
        elif tag.lower() == "select":
            self._select_name = attributes.get("name", "")
        elif tag.lower() == "option" and self._select_name:
            self.form_fields.append(
                {
                    "name": self._select_name,
                    "value": attributes.get("value", ""),
                    "type": "option",
                    "selected": "selected" in attributes,
                }
            )
        if tag.lower() == "title":
            self._in_title = True
        if tag.lower() in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self._in_heading = True
            self._heading_parts = []
        if tag.lower() == "table":
            self._table_depth += 1
            if self._table_depth == 1:
                self._table = {"rows": [], "title": self._last_heading}
        elif self._table_depth == 1 and tag.lower() == "tr":
            self._row = {"cells": []}
        elif self._table_depth == 1 and tag.lower() in {"th", "td"} and self._row is not None:
            self._cell = {"kind": tag.lower(), "parts": [], "links": [], "assets": []}
        elif self._table_depth == 1 and tag.lower() == "a" and self._cell is not None:
            self._cell["links"].append(
                {
                    "text": "",
                    "href": attributes.get("href", ""),
                    "onclick": attributes.get("onclick", ""),
                    "company_id": attributes.get("data-cmpy-id", ""),
                }
            )
        elif self._table_depth == 1 and tag.lower() == "img" and self._cell is not None:
            self._cell["assets"].append(
                {
                    "src": attributes.get("src", ""),
                    "alt": attributes.get("alt", ""),
                    "title": attributes.get("title", ""),
                }
            )
        if tag.lower() in {"a", "iframe", "embed", "object", "source", "img"}:
            address = attributes.get("href") or attributes.get("src") or attributes.get("data")
            if address or (tag.lower() == "a" and attributes.get("onclick")):
                self.links.append(
                    {
                        "href": address,
                        "onclick": attributes.get("onclick", ""),
                        "text": "",
                        "company_id": attributes.get("data-cmpy-id", ""),
                    }
                )

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"script", "style", "noscript"} and self._skip_depth:
            self._skip_depth -= 1
            return
        if self._skip_depth:
            return
        if tag == "select":
            self._select_name = ""
        if tag == "title":
            self._in_title = False
        if tag in {"h1", "h2", "h3", "h4", "h5", "h6"} and self._in_heading:
            heading = _clean(" ".join(self._heading_parts))
            if heading:
                self._last_heading = heading
            self._in_heading = False
        if tag == "table" and self._table_depth:
            self._table_depth -= 1
            if self._table_depth == 0 and self._table is not None:
                self.tables.append(self._table)
                self._table = None
        elif self._table_depth == 1 and tag in {"th", "td"} and self._cell is not None:
            self._cell["text"] = _clean(" ".join(self._cell.pop("parts")))
            if self._row is not None:
                self._row["cells"].append(self._cell)
            self._cell = None
        elif self._table_depth == 1 and tag == "tr" and self._row is not None:
            if self._table is not None:
                self._table["rows"].append(self._row)
            self._row = None

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        if self._in_title:
            self.title_parts.append(data)
        if self._in_heading:
            self._heading_parts.append(data)
        if data.strip():
            self.text_parts.append(data.strip())
        if self._cell is not None:
            self._cell["parts"].append(data)
            if self._cell["links"]:
                self._cell["links"][-1]["text"] += data


def parse_html_records(html: str, dataset: str, source_url: str) -> tuple[list[dict[str, Any]], list[str]]:
    """Return one page snapshot and normalized rows from each HTML table."""
    parser = _TableParser()
    parser.feed(html)
    parser.close()

    links: list[str] = []
    base_host = urlparse(source_url).hostname
    for link in parser.links:
        candidate = link.get("href", "")
        if candidate.startswith("javascript:") or candidate.startswith("#"):
            candidate = ""
        if not candidate and link.get("onclick"):
            edge_no = re.search(
                r"(?:edge_no=|openDiscViewer\(['\"]?)([A-Za-z0-9_-]+)",
                link["onclick"],
            )
            if edge_no:
                candidate = f"/openDiscViewer.do?edge_no={edge_no.group(1)}"
        if candidate:
            absolute = urljoin(source_url, candidate)
            parsed = urlparse(absolute)
            if parsed.scheme in {"http", "https"} and (
                parsed.hostname == base_host
                or bool(parsed.hostname and parsed.hostname.endswith(".pse.com.ph"))
            ):
                links.append(absolute)

    records: list[dict[str, Any]] = [
        {
            "record_type": "page_snapshot",
            "title": _clean(" ".join(parser.title_parts)),
            "page_text": _clean(" ".join(parser.text_parts))[:100_000],
            "table_count": len(parser.tables),
            "form_fields": parser.form_fields,
        }
    ]

    for table_no, table in enumerate(parser.tables, start=1):
        rows = table["rows"]
        header_index = next(
            (index for index, row in enumerate(rows) if row["cells"] and all(cell["kind"] == "th" for cell in row["cells"])),
            None,
        )
        headers: list[str] = []
        if header_index is not None:
            headers = [cell["text"] or f"column_{index + 1}" for index, cell in enumerate(rows[header_index]["cells"])]
        data_start = header_index + 1 if header_index is not None else 0

        for row_no, row in enumerate(rows[data_start:], start=1):
            cells = row["cells"]
            values = [cell["text"] for cell in cells]
            if not any(values):
                continue
            if not headers:
                headers = [f"column_{index + 1}" for index in range(len(values))]
            columns = {
                (headers[index] if index < len(headers) else f"column_{index + 1}"): value
                for index, value in enumerate(values)
            }
            cell_links = [link for cell in cells for link in cell["links"]]
            if _clean(" ".join(values)).casefold() == _clean(" ".join(headers)).casefold():
                continue
            records.append(
                {
                    "record_type": "table_row",
                    "table_no": table_no,
                    "row_no": row_no,
                    "table_title": table.get("title", ""),
                    "columns": columns,
                    "cells": values,
                    "links": cell_links,
                    "assets": [asset for cell in cells for asset in cell["assets"]],
                }
            )

    return records, sorted(set(links))


def parse_rss_records(xml_bytes: bytes, feed_url: str) -> list[dict[str, Any]]:
    """Parse RSS 2.0, RDF/RSS 1.0, or Atom documents into feed item records."""
    root = ET.fromstring(xml_bytes)
    records: list[dict[str, Any]] = []

    def local_name(tag: str) -> str:
        return tag.rsplit("}", 1)[-1].lower()

    for item in (node for node in root.iter() if local_name(node.tag) == "item"):
        values: dict[str, Any] = {}
        for child in item:
            key = local_name(child.tag)
            value = "".join(child.itertext()).strip()
            if key == "link":
                value = child.attrib.get("href") or value
            if key == "category":
                values.setdefault("categories", []).append(value)
            elif key in {"guid", "id"}:
                values["id"] = value
            elif key in {"pubdate", "published", "updated", "date"}:
                values["published_at"] = value
            elif key in {"description", "summary", "content", "encoded"}:
                values["description"] = value
            else:
                values[key] = value
        if not values.get("link"):
            values["link"] = values.get("guid", "")
        values.setdefault("id", values.get("link") or values.get("title", ""))
        values["feed_url"] = feed_url
        records.append({"record_type": "rss_item", **values})

    # Atom feeds use <entry>, links are attributes, and field names are namespaced.
    for item in (node for node in root.iter() if local_name(node.tag) == "entry"):
        values: dict[str, Any] = {"record_type": "rss_item", "feed_url": feed_url}
        categories: list[str] = []
        for child in item:
            key = local_name(child.tag)
            if key == "link":
                if child.attrib.get("rel", "alternate") == "alternate":
                    values["link"] = child.attrib.get("href", "")
            elif key == "category":
                categories.append(child.attrib.get("term", ""))
            elif key in {"id", "title", "summary", "content", "published", "updated", "author"}:
                values[key] = " ".join("".join(child.itertext()).split())
        if categories:
            values["categories"] = categories
        values["id"] = values.get("id") or values.get("link") or values.get("title", "")
        values["published_at"] = values.get("published") or values.get("updated", "")
        values["description"] = values.get("summary") or values.get("content", "")
        records.append(values)

    return records


def parse_json_records(json_bytes: bytes, dataset: str, source_url: str) -> list[dict[str, Any]]:
    """Keep JSON API responses structured while preserving their source shape."""
    payload = json.loads(json_bytes)
    if isinstance(payload, list):
        rows = payload
        metadata: dict[str, Any] = {}
    elif isinstance(payload, dict):
        metadata = {key: value for key, value in payload.items() if not isinstance(value, list)}
        rows = next(
            (value for value in payload.values() if isinstance(value, list) and value and isinstance(value[0], dict)),
            [],
        )
    else:
        return [{"record_type": "json_value", "value": payload}]

    records: list[dict[str, Any]] = [
        {
            "record_type": "json_snapshot",
            "metadata": metadata,
            "source_url": source_url,
            "row_count": len(rows),
        }
    ]
    for row_no, row in enumerate(rows, start=1):
        records.append({"record_type": "json_row", "row_no": row_no, "data": row})
    return records

