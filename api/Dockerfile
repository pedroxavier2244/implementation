FROM python:3.12-slim
WORKDIR /app
COPY requirements/api.txt requirements/api.txt
COPY requirements/base.txt requirements/base.txt
RUN pip install --no-cache-dir -r requirements/api.txt
COPY alembic.ini alembic.ini
COPY migrations/ migrations/
COPY scripts/ scripts/
COPY shared/ shared/
COPY api/ api/
EXPOSE 8000
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
