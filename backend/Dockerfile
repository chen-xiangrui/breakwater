FROM python:3.12-slim
WORKDIR /app
COPY requirements.lock pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.lock
COPY breakwater ./breakwater
RUN pip install --no-cache-dir --no-deps .
COPY . .
ENV PYTHONUNBUFFERED=1
CMD ["uvicorn", "breakwater.api:app", "--host", "0.0.0.0", "--port", "8000"]

