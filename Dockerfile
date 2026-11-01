# Policy ETL: PySpark pipeline (raw JSON -> bronze -> silver -> gold) in one portable image.

# Debian 12 (bookworm) base: it packages Java 17, which Spark 3.5 needs.
FROM python:3.11-slim-bookworm AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install the heavy dependency first, in its own layer: code changes then rebuild in seconds
RUN pip install "pyspark==3.5.6"

COPY pyproject.toml ./
COPY jobs/ ./jobs/
RUN pip install --no-deps .

COPY sql/ ./sql/

RUN useradd --create-home --uid 1000 etl && mkdir -p /app/data && chown -R etl /app
USER etl

ENTRYPOINT ["python", "-m"]
CMD ["policy_etl.etl_pipeline"]


FROM runtime AS test
USER root
RUN pip install "pytest>=8" "pytest-cov>=5"
COPY tests/ ./tests/
RUN chown -R etl /app
USER etl
CMD ["pytest", "--cov=policy_etl", "--cov-report=term-missing"]
