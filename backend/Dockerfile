# Share the same Python environment across the API, dispatcher, worker, and migrations.
FROM python:3.12-slim
WORKDIR /app
# Cache dependency installation separately from frequently edited application files.
COPY requirements.lock pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.lock
# Install the package without resolving dependencies again, then include tests/migrations.
COPY breakwater ./breakwater
RUN pip install --no-cache-dir --no-deps .
COPY . .
# Emit logs immediately; Compose overrides the API command for the other backend roles.
ENV PYTHONUNBUFFERED=1
CMD ["uvicorn", "breakwater.api:app", "--host", "0.0.0.0", "--port", "8000"]

