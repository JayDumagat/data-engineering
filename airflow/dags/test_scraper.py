from airflow.sdk import dag, task
from datetime import datetime

from pipeline.sources.pse_edge.test_fetch import scrape


@dag(
    schedule="*/6 * * * *",
    start_date=datetime(2026, 1, 2),
    catchup=False,
)
def test_scraper():

    @task(queue="scrape-light")
    def scrape_website():
        result = scrape()
        print(result)

    scrape_website()


test_scraper()