FROM python:3.12-slim
WORKDIR /app
COPY requirements/notifier.txt requirements/notifier.txt
COPY requirements/base.txt requirements/base.txt
RUN pip install --no-cache-dir -r requirements/notifier.txt
COPY shared/ shared/
COPY checker/ checker/
COPY notifier/ notifier/
CMD ["celery", "-A", "notifier.celery_app", "worker", "-Q", "notification_jobs", "-c", "4", "--loglevel=info"]
