FROM python:3.12-slim
WORKDIR /app
COPY requirements/worker.txt requirements/worker.txt
COPY requirements/base.txt requirements/base.txt
RUN pip install --no-cache-dir -r requirements/worker.txt
COPY shared/ shared/
COPY worker/ worker/
RUN useradd -r -m -u 1001 celeryuser
USER celeryuser
CMD ["celery", "-A", "worker.celery_app", "worker", "-Q", "etl_jobs", "-c", "2", "--loglevel=info"]
