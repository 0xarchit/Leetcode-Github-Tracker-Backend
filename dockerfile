# Multi-stage: slim builder + distroless runtime for a small image

FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1 \
  PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN pip install --upgrade pip

# Install deps first for better layer caching into a portable target dir
COPY requirements.txt ./
RUN pip install --no-cache-dir --target /opt/python -r requirements.txt

# Copy application code
COPY . .


# Distroless final image
FROM gcr.io/distroless/python3-debian12:nonroot

ENV PYTHONUNBUFFERED=1 \
  PYTHONPATH="/opt/python"

WORKDIR /app

COPY --from=builder /opt/python /opt/python
COPY --from=builder /app /app

EXPOSE 8000

# Run as nonroot (provided by base image)
USER nonroot

# Distroless python images use the embedded python as ENTRYPOINT, so pass module args only
CMD ["-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
