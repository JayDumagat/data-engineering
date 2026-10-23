"""Convert PSE website captures into shared normalized records."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping
from urllib.parse import urljoin, urlparse
import re

from pipeline.etl.pse_edge.normalize import normalize_object, normalize_payload
from pipeline.sources.pse_website.catalog import ALLOWED_HOSTS


class PSEWebsiteTransformer:
    @staticmethod
    def _value(columns: dict[str, Any], *names: str) -> Any:
        for name in names:
            value = columns.get(name.casefold())
            if value is not None and str(value).strip():
                return value
        return None

    @staticmethod
    def _date(value: Any) -> str | None:
        if value is None or not str(value).strip():
            return None
        text = re.sub(r"\s+,", ",", str(value).strip())
        date_range_end = re.search(r"(?:-|–|—)\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})$", text)
        if date_range_end:
            text = date_range_end.group(1)
        for pattern in (
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%B %d, %Y",
            "%b %d, %Y",
            "%A, %B %d, %Y",
            "%A, %b %d, %Y",
        ):
            try:
                return datetime.strptime(text, pattern).date().isoformat()
            except ValueError:
                continue
        return text

    @staticmethod
    def _number(value: Any) -> int | float | None:
        if value is None:
            return None
        text = str(value).strip().replace(",", "")
        if not text or text in {"-", "–", "—", "N/A", "n/a"}:
            return None
        if not re.fullmatch(r"-?\d+(?:\.\d+)?", text):
            return None
        number = float(text)
        return int(number) if number.is_integer() else number

    @staticmethod
    def _clean_record(dataset: str, payload: dict[str, Any], source_url: str) -> dict[str, Any]:
        """Give source rows a domain record type and useful display fields."""
        record = dict(payload)
        if record.get("record_type") != "table_row":
            return record

        columns = record.get("columns", {})
        if not isinstance(columns, dict):
            columns = {}
        normalized_columns = {str(key).strip().casefold(): value for key, value in columns.items()}
        links = record.get("links", [])
        if not isinstance(links, list):
            links = []
        official_links = []
        for link in links:
            if not isinstance(link, dict) or not link.get("href"):
                continue
            href = urljoin(source_url, str(link["href"]).strip())
            if (urlparse(href).hostname or "").lower() in ALLOWED_HOSTS:
                official_links.append({**link, "href": href})
        record["links"] = official_links
        download_url = next((link["href"] for link in official_links), None)
        title = next(
            (
                str(normalized_columns[key]).strip()
                for key in ("title", "report type", "announcement", "article", "company name", "company", "name", "report")
                if normalized_columns.get(key) and str(normalized_columns[key]).strip()
            ),
            None,
        )
        if title is None:
            title = next(
                (str(value).strip() for value in columns.values() if value and str(value).strip()),
                None,
            )
        record_type = {
            "market_reports": "market_report",
            "announcements": "announcement",
            "press_releases": "press_release",
            "listing_statistics": "listing_statistic",
        }.get(dataset, "source_table_row")
        table_title = str(record.get("table_title") or "").strip() or None
        if dataset in {"announcements", "press_releases"} and not (
            title and PSEWebsiteTransformer._value(normalized_columns, "date", "published", "date published")
        ):
            record_type = "source_table_row"
        elif dataset == "market_reports" and not (
            PSEWebsiteTransformer._value(normalized_columns, "report type")
            and PSEWebsiteTransformer._value(normalized_columns, "report", "report date")
        ):
            record_type = "source_table_row"
        elif dataset == "listing_statistics" and not (
            normalized_columns.get("year")
            and table_title
            and any(
                PSEWebsiteTransformer._number(normalized_columns.get(name)) is not None
                for name in ("main", "sme", "total")
            )
        ):
            record_type = "source_table_row"

        record["record_type"] = record_type
        record["title"] = title
        record["source_dataset"] = dataset
        if download_url:
            link_field = "detail_url" if dataset in {"announcements", "press_releases"} else "download_url"
            record[link_field] = download_url

        if dataset == "market_reports":
            record["report_type"] = PSEWebsiteTransformer._value(normalized_columns, "report type") or title
            record["title"] = record["report_type"] or title
            record["report_period"] = PSEWebsiteTransformer._value(
                normalized_columns, "report", "report date"
            )
            record["report_date"] = PSEWebsiteTransformer._date(
                record["report_period"]
            )
            record["uploaded_date"] = PSEWebsiteTransformer._date(
                PSEWebsiteTransformer._value(normalized_columns, "date uploaded", "uploaded", "upload date")
            )
        elif dataset == "announcements":
            record["category"] = PSEWebsiteTransformer._value(normalized_columns, "type", "category")
            record["published_at"] = PSEWebsiteTransformer._date(
                PSEWebsiteTransformer._value(normalized_columns, "date", "published", "date published")
            )
            record["year"] = PSEWebsiteTransformer._number(
                PSEWebsiteTransformer._value(normalized_columns, "hf:tax:years", "year")
            )
        elif dataset == "press_releases":
            record["published_at"] = PSEWebsiteTransformer._date(
                PSEWebsiteTransformer._value(normalized_columns, "date", "published", "date published")
            )
            record["category"] = PSEWebsiteTransformer._value(
                normalized_columns, "hf:categories", "category", "type"
            )
            record["year"] = PSEWebsiteTransformer._number(
                PSEWebsiteTransformer._value(normalized_columns, "year", "hf:tax:years")
            )
        elif dataset == "listing_statistics":
            statistic_name = table_title or "Listed-company statistics"
            year = PSEWebsiteTransformer._number(normalized_columns.get("year"))
            record["statistic_name"] = statistic_name
            record["year"] = year
            record["title"] = f"{statistic_name} {year}" if year is not None else statistic_name
            for name in ("main", "sme", "total"):
                record[name] = PSEWebsiteTransformer._number(normalized_columns.get(name))
        return record

    def transform(
        self,
        descriptor: Mapping[str, Any],
        body: bytes | None,
        ingested_at: datetime,
    ) -> list[dict[str, Any]]:
        normalized = dict(descriptor)
        if descriptor.get("inline_records") is not None:
            payloads = list(descriptor["inline_records"])
        else:
            if body is None:
                raise ValueError("Raw body is required for this PSE website object")
            payloads = [row["payload"] for row in normalize_object(normalized, body, ingested_at)]
        dataset = str(descriptor["dataset"])
        records = []
        for payload in payloads:
            cleaned = self._clean_record(dataset, payload, str(descriptor["url"]))
            if dataset in {"market_reports", "announcements", "press_releases", "listing_statistics"}:
                if cleaned.get("record_type") == "page_snapshot":
                    continue
            records.append(normalize_payload(normalized, cleaned, ingested_at))
        return records
