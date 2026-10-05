from airflow.sdk import dag, task
from datetime import datetime

from pipeline.sources.pse_edge.scraper import scrape


@dag(
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def pse_edge_scraper():

    @task(queue="scrape-light")
    def scrape_website():
        result = scrape()
        print(result)

    scrape_website()


pse_edge_scraper()