FROM python:3.13

WORKDIR /service

# Add build tools needed for pydantic-core Rust compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies first (layer cached unless requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
