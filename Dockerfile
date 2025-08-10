FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies with memory optimization
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p uploads chroma_db

# Set environment variables for memory optimization
ENV PYTHONUNBUFFERED=1
ENV TRANSFORMERS_CACHE=/tmp/transformers_cache
ENV HF_HOME=/tmp/hf_cache

# Expose port
EXPOSE 5000

# Use a startup script to handle memory issues
CMD ["python", "-m", "gunicorn", "--bind", "0.0.0.0:5000", "--workers=1", "--threads=2", "--timeout=120", "--preload", "app_enhanced:app"]
