"""Public PSE EDGE page and search endpoint catalog."""

PAGE_SOURCES: dict[str, str] = {
    "home": "/",
    "site_map": "/page/siteMap.do",
    "company_directory_page": "/companyDirectory/form.do",
    "listing_applicants": "/disclosureData/listing_applicants_list.do",
    "watch_list": "/disclosureData/watchlist_view.do",
    "psei": "/psei/form.do",
    "etf": "/disclosureData/etf_form.do",
    "index_summary": "/index/form.do",
    "dividends_rights_page": "/disclosureData/dividends_and_rights_info_form.do",
    "halts_suspensions": "/disclosureData/halts_and_suspensions_list.do",
    "market_calendar": "/companyPage/marketCalendar.do",
    "company_announcements_page": "/announcements/form.do",
    "financial_reports_page": "/financialReports/form.do",
    "other_reports_page": "/otherReports/form.do",
    "disclosure_notices_page": "/disclosureNotices/form.do",
    "listing_notices_page": "/listingNotices/form.do",
}

SEARCH_SOURCES: dict[str, str] = {
    "company_directory": "/companyDirectory/search.ax",
    "company_announcements": "/announcements/search.ax",
    "financial_reports": "/financialReports/search.ax",
    "other_reports": "/otherReports/search.ax",
    "disclosure_notices": "/disclosureNotices/search.ax",
    "listing_notices": "/listingNotices/search.ax",
    "company_disclosures": "/companyDisclosures/search.ax",
    "dividends": "/disclosureData/dividends_and_rights_info_list.ax",
    "stock_rights": "/disclosureData/dividends_and_rights_info_list.ax",
}

DOCUMENT_PATHS = (
    "/openDiscViewer.do",
    "/downloadHtml.do",
    "/downloadPdf.do",
    "/download",
    "/attachments/",
)
