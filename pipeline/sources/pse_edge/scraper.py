from scrapling.fetchers import Fetcher


def scrape():
    page = Fetcher.get("https://edge.pse.com.ph/")

    return {
        "html": page.html,
    }