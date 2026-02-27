FROM python:3.12-slim
WORKDIR /app
COPY requirements/worker.txt requirements/worker.txt
COPY requirements/base.txt requirements/base.txt
RUN pip install --no-cache-dir -r requirements/worker.txt
COPY shared/ shared/
COPY worker/ worker/
