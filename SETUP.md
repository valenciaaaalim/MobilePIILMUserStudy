# Setup Guide

## Backend Setup (GLiNER Service)

### Deploy to Cloud Run

1. **Prerequisites**:
   - Google Cloud SDK installed
   - GCP project with billing enabled
   - Docker installed

2. **Deploy the backend**:
   ```bash
   cd backend
   export GOOGLE_CLOUD_PROJECT=your-project-id
   export REGION=us-central1
   ./deploy.sh
   ```

3. **Set up Cloudflare** (see `backend/cloudflare-config.md` for details):
   - Add your domain to Cloudflare
   - Create CNAME pointing to Cloud Run service
   - Configure WAF rules and rate limiting
   - Enable security features

4. **Get your backend URL**:
   - Cloud Run URL: `https://your-service-url.run.app`
   - Or Cloudflare URL: `https://your-domain.com`

## Android App Configuration

### API Keys Setup

1. **Get your Gemini API key** from [Google AI Studio](https://makersuite.google.com/app/apikey)

2. **Create secrets.properties file**:
   - Copy `app/secrets.properties.example` to `app/secrets.properties`
   - Add your keys:
     ```
     GEMINI_API_KEY=your_gemini_api_key_here
     BACKEND_URL=https://your-backend-url.com
     BACKEND_API_KEY=your_backend_api_key_here  # Optional
     ```

3. **Verify gitignore**: The `secrets.properties` file is already in `.gitignore` to protect your API keys.

## How It Works

### Pipeline Flow

1. **User types message** → Debounce (1500ms)
2. **GLiNER Preprocessing** → Masks PII and chunks text
3. **Stage 1: Analysis Prompt** → Sends masked conversation to Gemini API using `prompt.md` template
4. **Stage 2: Risk Assessment** → Takes Stage 1 JSON output and sends to Gemini API using `risk_assessment.md` template
5. **Warning Display** → Shows warning modal if risk >= Medium

### Concurrency Control

- The pipeline uses a mutex to prevent multiple simultaneous API calls
- If user continues typing while a request is in progress, the new request will be queued/cancelled appropriately
- Each request has a unique ID to track and cancel if needed

### GLiNER Integration

The app automatically uses the backend GLiNER service if `BACKEND_URL` is configured in `secrets.properties`.

**Backend Service** (Recommended - Already Implemented):
- FastAPI backend deployed to Cloud Run
- Cloudflare in front for security hardening
- Automatically used when `BACKEND_URL` is set
- Falls back to stub if backend is unavailable

**Fallback Behavior**:
- If `BACKEND_URL` is not set, uses `StubGliNERService` (basic chunking, no PII masking)
- If backend is unavailable, errors are handled gracefully

### XML Conversation History

The `formatConversationHistoryAsXml()` function currently uses a placeholder format. When you integrate XML formatting code, replace the placeholder implementation in `RiskAssessmentPipeline.kt`.

## Testing

- Without API key: App runs with stubbed implementations (no warnings shown)
- With API key: Full pipeline runs, but GLiNER is still stubbed
- To test warnings: Modify `StubRiskAssessmentHook` to return a mock `WarningState`


## Start the local host
```
conda activate ner

cd backend
uvicorn app.main:app --host 0.0.0.0 --port 8080
access via PostgreSQL via DATABASE_URL

cd frontend
npm install
npm start
access via http://localhost:3000
```
