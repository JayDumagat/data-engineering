FROM apache/airflow:3.3.2-python3.12

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12

ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

USER airflow

COPY --chown=airflow:root requirements.txt /requirements.txt

RUN pip install \
    --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install \
    --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements.txt

RUN python -c "import airflow; print('Airflow:', airflow.__version__)"
RUN python -c "import scrapling; print('Scrapling:', scrapling.__version__)"
RUN python -c "from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager; print('FAB OK')"
RUN python -c "from airflow.providers.amazon.aws.log.s3_task_handler import S3TaskHandler; print('Amazon S3 task logging provider OK')"

USER root

ENV PATH="/home/airflow/.local/bin:${PATH}"
ENV PYTHONPATH="/home/airflow/.local/lib/python3.12/site-packages"

RUN mkdir -p "${PLAYWRIGHT_BROWSERS_PATH}" \
    && chown -R airflow:root "${PLAYWRIGHT_BROWSERS_PATH}" \
    && chmod -R 775 "${PLAYWRIGHT_BROWSERS_PATH}"

RUN scrapling install

RUN chown -R airflow:root "${PLAYWRIGHT_BROWSERS_PATH}" \
    && chmod -R g+rwX "${PLAYWRIGHT_BROWSERS_PATH}"

USER airflow

RUN python -c "from scrapling.fetchers import Fetcher, FetcherSession, DynamicFetcher, StealthyFetcher; print('Scrapling fetchers and sessions OK')"
