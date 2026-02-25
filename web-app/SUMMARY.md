# Web App Conversion Summary

This document summarizes the conversion of the native Android app to a web application for user testing.

## Completed Components

### Backend (FastAPI)

✅ **Database Schema**
- Participant model with A/B testing assignment
- ConversationSession model for tracking user progress
- UserInput model for capturing pre-click and final submitted text
- SurveyResponse model for survey data
- Conversation model for seed data
- AuditLog model for analytics

✅ **API Endpoints**
- Participant management (create, get) with balanced A/B assignment
- Conversation endpoints (seed data, sessions)
- Risk assessment endpoint (integrates with Gemini API)
- User input capture endpoints (with and without warning data)
- Survey response endpoints
- Prolific completion routing

✅ **Services**
- Gemini API service with abstraction layer
- Risk assessment pipeline service
- Template loading for prompts

✅ **Security**
- Security headers middleware (X-Content-Type-Options, X-Frame-Options, etc.)
- CORS configuration
- Input validation via Pydantic
- Environment-based secrets
- HTTPS-only cookies support (configurable)

✅ **Database Seeding**
- Script/runtime loader for conversations from `backend/app/assets/annotated_test.json`
- Handles all three conversation scenarios (1000, 1001, 1002)

### Frontend (React)

✅ **Screens**
- WelcomeScreen with instructions
- ConversationScreen with WhatsApp-like UI
- SurveyScreen for mid/post conversation surveys
- CompletionScreen with Prolific redirect

✅ **Components**
- ChatHeader (WhatsApp-style header)
- MessageList with scrolling
- MessageBubble (sent/received styling)
- DateSeparator
- ChatComposer (input field with send button)
- WarningModal (risk warnings with rewrite/continue options)

✅ **Features**
- Conversation flow management
- Risk assessment integration
- User input capture (pre-click and final text)
- Survey question handling
- A/B variant display

### Infrastructure

✅ **Docker Setup**
- Backend Dockerfile
- XML Extractor stub service
- Docker Compose with networking
- Health checks for both services

✅ **Deployment**
- Google Cloud Run deployment documentation
- Cloudflare configuration guide
- Environment variable management
- Secrets management guidance

## Architecture Decisions

1. **Database**: Started with SQLite for simplicity, PostgreSQL recommended for production
2. **Frontend Routing**: React Router for navigation between screens
3. **API Communication**: Axios for HTTP requests
4. **State Management**: React hooks (useState, useEffect) for local state
5. **Styling**: CSS modules for component styling, WhatsApp-like design
6. **Security**: Middleware-based security headers, CORS, input validation

## Data Flow

1. User arrives → Participant created → A/B variant assigned
2. Welcome screen → Instructions displayed
3. Conversation 1 → User types → Risk assessment → Warning (if needed) → User input captured
4. Survey (mid) → Responses captured
5. Repeat for conversations 2 and 3
6. Final survey → Completion screen → Prolific redirect

## Key Features Implemented

- ✅ Three conversation scenarios from `backend/app/assets/annotated_test.json`
- ✅ A/B testing with balanced assignment
- ✅ User input capture (pre-click and final text)
- ✅ Risk assessment with Gemini API
- ✅ Warning modals with rewrite/continue options
- ✅ Survey questions between conversations
- ✅ Prolific completion URL routing
- ✅ Database persistence of all interactions
- ✅ Security best practices

## Testing Flow

1. Participant ID can be passed via URL parameter (`?PROLIFIC_PID=...`)
2. Participant is assigned to variant A or B (balanced)
3. User goes through three conversations sequentially
4. After each conversation, user answers survey questions
5. All user inputs, survey responses, and actions are logged
6. User is redirected to Prolific completion URL at the end

## Next Steps / Improvements

1. **XML Extractor Integration**: Currently stubbed, needs full implementation
2. **GLiNER Integration**: PII masking before risk assessment (currently uses original text)
3. **Enhanced Rewrite Generation**: Currently returns original text, should use LLM to generate safer alternatives
4. **Frontend Deployment**: Add frontend Dockerfile and deployment instructions
5. **Production Database**: Migrate from SQLite to PostgreSQL for production
6. **Advanced CSRF Protection**: Implement full CSRF token validation
7. **Rate Limiting**: Add rate limiting middleware
8. **Monitoring**: Add structured logging and monitoring integration

## Files Created

### Backend
- `web-app/backend/app/main.py` - FastAPI application
- `web-app/backend/app/config.py` - Configuration
- `web-app/backend/app/database.py` - Database setup
- `web-app/backend/app/models.py` - Database models
- `web-app/backend/app/schemas.py` - Pydantic schemas
- `web-app/backend/app/routers/` - API route handlers
- `web-app/backend/app/services/` - Business logic services
- `web-app/backend/app/middleware/security.py` - Security middleware
- `web-app/backend/app/assets/` - Prompt templates
- `web-app/backend/seed_data.py` - Database seeding script
- `web-app/backend/Dockerfile` - Container image
- `web-app/backend/requirements.txt` - Dependencies

### Frontend
- `web-app/frontend/src/App.js` - Main app component
- `web-app/frontend/src/components/` - React components
- `web-app/frontend/package.json` - Dependencies
- `web-app/frontend/public/index.html` - HTML template

### Infrastructure
- `web-app/docker-compose.yml` - Multi-service setup
- `web-app/xml-extractor/` - Stub XML extractor service
- `web-app/README.md` - Developer guide
- `web-app/DEPLOYMENT.md` - Deployment instructions

## Notes

- The XML extractor service is currently a stub and returns placeholder XML
- Conversation direction logic determines SENT vs RECEIVED based on which participant name appears first
- Risk assessment currently uses unmasked text (GLiNER integration pending)
- Frontend is designed to run separately from backend (different ports, CORS enabled)
