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

# Pre-download GLiNER model and tokenizer so first request doesn't block
# This can take several minutes - be patient!
RUN python - <<'PY'
import os
import sys
from gliner import GLiNER
from transformers import AutoTokenizer

model_name = os.environ.get("GLINER_MODEL_NAME", "knowledgator/gliner-pii-base-v1.0")
print(f"[BUILD] Starting GLiNER model download: {model_name}", flush=True)
print("[BUILD] This may take 2-5 minutes depending on network speed...", flush=True)

try:
    print("[BUILD] Downloading GLiNER model...", flush=True)
    model = GLiNER.from_pretrained(model_name)
    print("[BUILD] ✓ GLiNER model downloaded successfully", flush=True)
    
    print("[BUILD] Downloading tokenizer...", flush=True)
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    print("[BUILD] ✓ Tokenizer downloaded successfully", flush=True)
    
    print(f"[BUILD] ✓ Pre-downloaded GLiNER model: {model_name}", flush=True)
except Exception as e:
    print(f"[BUILD] ERROR downloading model: {e}", flush=True)
    print("[BUILD] Model will be downloaded on first use instead", flush=True)
    sys.exit(0)  # Don't fail the build, download on first use
PY

# Expose port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
