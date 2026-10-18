"""Declarative inventory of public PSE EDGE pages and data endpoints."""

from dataclasses import dataclass


# Static routes published by the PSE EDGE site map, plus the public footer.
PAGE_SOURCES: dict[str, str] = {
    "home": "/",
    "site_map": "/page/siteMap.do",
    "about": "/page/aboutPseEdge.do",
    "disclaimer": "/page/disclaimer.do",
    "privacy_policy": "/page/privacyPolicy.do",
    "link_policy": "/page/linkPolicy.do",
    "contact_us": "/page/contactUs.do",
    "faqs": "/page/faqs.do",
    "company_directory_page": "/companyDirectory/form.do",
    "listing_applicants": "/disclosureData/listing_applicants_list.do",
    "watch_list": "/disclosureData/watchlist_view.do",
    "psei_search_page": "/psei/form.do",
    "etf_search_page": "/disclosureData/etf_form.do",
    "index_summary": "/index/form.do",
    "dividends_rights_page": "/disclosureData/dividends_and_rights_info_form.do",
    "halts_suspensions": "/disclosureData/halts_and_suspensions_list.do",
    "market_calendar": "/companyPage/marketCalendar.do",
    "company_announcements_page": "/announcements/form.do",
    "financial_reports_page": "/financialReports/form.do",
    "other_reports_page": "/otherReports/form.do",
    "disclosure_notices_page": "/disclosureNotices/form.do",
    "listing_notices_page": "/listingNotices/form.do",
    "advanced_search_page": "/keyword/form.do",
}


@dataclass(frozen=True)
class SearchSource:
    """Request shape for one independently paginated PSE EDGE search."""

    path: str
    form_fields: tuple[tuple[str, str], ...] = ()
    query_params: tuple[tuple[str, str], ...] = ()
    page_field: str = "pageNo"

    def fields_for_page(
        self,
        page_no: int,
        *,
        date_window: tuple[str, str] | None = None,
    ) -> dict[str, str | int]:
        fields: dict[str, str | int] = dict(self.form_fields)
        if date_window is not None:
            if "fromDate" in fields:
                fields["fromDate"] = date_window[0]
            if "toDate" in fields:
                fields["toDate"] = date_window[1]
        fields[self.page_field] = page_no
        return fields


# The portal's searchList helper submits these .ax endpoints with form-encoded
# POST bodies. Defaults are the broadest public searches exposed by each form.
SEARCH_SOURCES: dict[str, SearchSource] = {
    "company_directory": SearchSource(
        "/companyDirectory/search.ax",
        form_fields=(
            ("companyId", ""),
            ("keyword", ""),
            ("sortType", ""),
            ("dateSortType", "DESC"),
            ("cmpySortType", "ASC"),
            ("symbolSortType", "ASC"),
            ("sector", "ALL"),
            ("subsector", "ALL"),
        ),
    ),
    "company_announcements": SearchSource(
        "/announcements/search.ax",
        form_fields=(("keyword", ""), ("tmplNm", ""), ("fromDate", ""), ("toDate", "")),
    ),
    "financial_reports": SearchSource(
        "/financialReports/search.ax",
        form_fields=(("keyword", ""), ("tmplNm", ""), ("fromDate", ""), ("toDate", "")),
    ),
    "other_reports": SearchSource(
        "/otherReports/search.ax",
        form_fields=(("keyword", ""), ("tmplNm", ""), ("fromDate", ""), ("toDate", "")),
    ),
    "disclosure_notices": SearchSource(
        "/disclosureNotices/search.ax",
        form_fields=(("keyword", ""), ("tmplNm", ""), ("fromDate", ""), ("toDate", "")),
    ),
    "listing_notices": SearchSource(
        "/listingNotices/search.ax",
        form_fields=(("keyword", ""), ("tmplNm", ""), ("fromDate", ""), ("toDate", "")),
    ),
    "company_disclosures": SearchSource(
        "/companyDisclosures/search.ax",
        form_fields=(
            ("keyword", ""),
            ("tmplNm", ""),
            ("sortType", ""),
            ("dateSortType", ""),
            ("cmpySortType", ""),
        ),
    ),
    # This form keeps its category in the URL and its page number in pageNum.
    "dividends": SearchSource(
        "/disclosureData/dividends_and_rights_info_list.ax",
        form_fields=(
            ("sortMode", "date"),
            ("dateSortType", "DESC"),
            ("cmpySortType", "ASC"),
        ),
        query_params=(("DividendsOrRights", "Dividends"),),
        page_field="pageNum",
    ),
    "stock_rights": SearchSource(
        "/disclosureData/dividends_and_rights_info_list.ax",
        form_fields=(
            ("sortMode", "date"),
            ("dateSortType", "DESC"),
            ("cmpySortType", "ASC"),
        ),
        query_params=(("DividendsOrRights", "Rights"),),
        page_field="pageNum",
    ),
    "psei_disclosures": SearchSource(
        "/psei/search.ax",
        form_fields=(
            ("companyId", ""),
            ("keyword", ""),
            ("fromDate", ""),
            ("toDate", ""),
            ("sortType", ""),
            ("dateSortType", ""),
            ("cmpySortType", ""),
        ),
    ),
    "etf_disclosures": SearchSource(
        "/disclosureData/etf_list.ax",
        form_fields=(("companyId", ""), ("keyword", ""), ("fromDate", ""), ("toDate", "")),
    ),
    "advanced_search": SearchSource(
        "/keyword/search.ax",
        form_fields=(
            ("keyword", "*"),
            ("fromDate", ""),
            ("toDate", ""),
            ("companyId", ""),
            ("cmpyNm", ""),
            ("subjectTitle", ""),
            ("sector", ""),
            ("subsector", ""),
        ),
    ),
}


# Company routes are parameterized by cmpy_id and are fetched once per issuer.
COMPANY_DETAIL_SOURCES: dict[str, str] = {
    "company_stock_data": "/companyPage/stockData.do",
    "company_information": "/companyInformation/form.do",
    "company_directors_and_management": "/companyPage/directors_and_management_list.do",
    "company_financial_reports": "/companyPage/financial_reports_view.do",
    "company_disclosures_page": "/companyDisclosures/form.do",
    "company_dividends_rights_page": "/companyPage/dividends_and_rights_form.do",
}

COMPANY_DIVIDENDS_SOURCE = "/companyPage/dividends_and_rights_list.ax"
PRICE_HISTORY_SOURCE = "/common/DisclosureCht.ax"
CALENDAR_START_YEAR = 2016

DOCUMENT_PATHS = (
    "/openDiscViewer.do",
    "/downloadHtml.do",
    "/downloadPdf.do",
    "/download",
    "/attachments/",
)


@dataclass(frozen=True)
class PSEEdgeJob:
    """Declarative source boundary for one independently runnable data product."""

    page_datasets: tuple[str, ...] = ()
    search_datasets: tuple[str, ...] = ()
    include_company_details: bool = False
    include_price_history: bool = False
    include_calendar_history: bool = False
    follow_documents: bool = False
    search_window_days: int | None = 14


PSE_EDGE_JOBS: dict[str, PSEEdgeJob] = {
    # Frequent current-state lanes stay small enough for a weekday schedule.
    "company_directory": PSEEdgeJob(
        page_datasets=("company_directory_page",),
        search_datasets=("company_directory",),
    ),
    "index_summary": PSEEdgeJob(page_datasets=("index_summary",)),
    "market_reference": PSEEdgeJob(
        page_datasets=(
            "home",
            "site_map",
            "about",
            "disclaimer",
            "privacy_policy",
            "link_policy",
            "contact_us",
            "faqs",
            "psei_search_page",
            "etf_search_page",
            "watch_list",
            "halts_suspensions",
            "market_calendar",
            "listing_applicants",
            "dividends_rights_page",
            "advanced_search_page",
        ),
        follow_documents=True,
    ),
    "company_announcements": PSEEdgeJob(
        page_datasets=("company_announcements_page",),
        search_datasets=("company_announcements",),
        follow_documents=True,
    ),
    "financial_reports": PSEEdgeJob(
        page_datasets=("financial_reports_page",),
        search_datasets=("financial_reports",),
        follow_documents=True,
    ),
    "other_reports": PSEEdgeJob(
        page_datasets=("other_reports_page",),
        search_datasets=("other_reports",),
        follow_documents=True,
    ),
    "disclosure_notices": PSEEdgeJob(
        page_datasets=("disclosure_notices_page",),
        search_datasets=("disclosure_notices",),
        follow_documents=True,
    ),
    "listing_notices": PSEEdgeJob(
        page_datasets=("listing_notices_page",),
        search_datasets=("listing_notices",),
        follow_documents=True,
    ),
    "company_disclosures": PSEEdgeJob(
        search_datasets=("company_disclosures",),
        follow_documents=True,
    ),
    "advanced_search": PSEEdgeJob(
        search_datasets=("advanced_search",),
        follow_documents=True,
    ),
    "psei_disclosures": PSEEdgeJob(search_datasets=("psei_disclosures",), follow_documents=True),
    "etf_disclosures": PSEEdgeJob(search_datasets=("etf_disclosures",), follow_documents=True),
    "dividends": PSEEdgeJob(search_datasets=("dividends",)),
    "stock_rights": PSEEdgeJob(search_datasets=("stock_rights",)),
    # Full issuer pages and history remain explicit backfill lanes.
    "company_profiles": PSEEdgeJob(
        page_datasets=("company_directory_page",),
        search_datasets=("company_directory",),
        include_company_details=True,
        follow_documents=True,
    ),
    "company_market_history": PSEEdgeJob(
        page_datasets=("company_directory_page",),
        search_datasets=("company_directory",),
        include_company_details=True,
        include_price_history=True,
        follow_documents=True,
    ),
    "market_calendar_archive": PSEEdgeJob(
        include_calendar_history=True,
    ),
}

BACKFILL_PSE_EDGE_JOBS = (
    "company_disclosures",
    "company_profiles",
    "company_market_history",
    "market_calendar_archive",
)
DEFAULT_PSE_EDGE_JOBS = tuple(
    name for name in PSE_EDGE_JOBS if name not in BACKFILL_PSE_EDGE_JOBS
)
PSE_EDGE_FULL_BACKFILL_JOBS = tuple(
    name for name in PSE_EDGE_JOBS if name not in {"company_directory", "company_profiles"}
)

# Explicit full capture: every catalog page/search, issuer route, discovered
# stock history, historical calendar month, and linked PSE-hosted document.
PSE_EDGE_CONFIGURED_SOURCES_JOB = PSEEdgeJob(
    page_datasets=tuple(PAGE_SOURCES),
    search_datasets=tuple(SEARCH_SOURCES),
    include_company_details=True,
    include_price_history=True,
    include_calendar_history=True,
    follow_documents=True,
    search_window_days=None,
)

PSE_EDGE_SCOPE_ALIASES: dict[str, str] = {"all": "configured_sources"}
PSE_EDGE_ALL_JOB = PSE_EDGE_CONFIGURED_SOURCES_JOB
