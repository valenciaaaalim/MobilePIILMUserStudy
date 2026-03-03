FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt



# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV GLINER_MODEL_NAME=knowledgator/gliner-pii-base-v1.0
ENV HF_HOME=/app/model_cache
ENV TRANSFORMERS_CACHE=/app/model_cache
ENV HF_HUB_CACHE=/app/model_cache

# Download NLTK data for sentence tokenization
RUN python -c "import nltk; nltk.download('punkt'); nltk.download('punkt_tab')"

# Pre-download GLiNER model at build-time (single-line command; no heredoc).
RUN mkdir -p /app/model_cache && \
    python -c "from gliner import GLiNER; from transformers import AutoTokenizer; m='knowledgator/gliner-pii-base-v1.0'; GLiNER.from_pretrained(m); AutoTokenizer.from_pretrained(m)"

# Copy application code
COPY app/ ./app/

# Copy GLiNER service module
COPY gliner_service.py .

# Copy seed script
COPY seed_data.py .

# Create assets directory if needed
RUN mkdir -p app/assets

# Expose port
EXPOSE 8080

# Run the application
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}"]
