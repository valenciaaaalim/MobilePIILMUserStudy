FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/

# Copy GLiNER service module
COPY gliner_service.py .

# Copy seed script
COPY seed_data.py .

# Create assets directory if needed
RUN mkdir -p app/assets

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV GLINER_MODEL_NAME=knowledgator/gliner-pii-base-v1.0

# Download NLTK data for sentence tokenization
RUN python -c "import nltk; nltk.download('punkt'); nltk.download('punkt_tab')"

# Optional model warmup at build-time removed because Docker heredoc syntax
# is not supported in this Cloud Build setup.

# Expose port
EXPOSE 8080

# Run the application
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}"]