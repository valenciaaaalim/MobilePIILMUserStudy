# WhatsApp Risk Assessment Web App

This is a web application for user testing of the WhatsApp risk assessment system, converted from the native Android app.

## Architecture

- **Backend**: FastAPI (Python) with SQLite/PostgreSQL database
- **Frontend**: React with React Router
- **Services**:
  - Gemini API integration for risk assessment
- **Deployment**: Docker containers, Google Cloud Run, Cloudflare

## Local Development

### Prerequisites

- Python 3.11+
- Node.js 16+ and npm
- Docker and Docker Compose (for full stack)

### Backend Setup

1. **Navigate to backend directory:**
   ```bash
   cd web-app/backend
   ```

2. **Create virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set environment variables:**
   Create a `.env` file:
   ```env
   GEMINI_API_KEY=your_gemini_api_key_here
   GEMINI_MODEL=gemini-3-flash-preview
   GEMINI_TIMEOUT_SECONDS=15
   GEMINI_MAX_ATTEMPTS=2
   GEMINI_LIVE_TIMEOUT_SECONDS=8
   GEMINI_LIVE_MAX_ATTEMPTS=1
   DATABASE_URL=sqlite:///./web_app.db
   FRONTEND_URL=http://localhost:3000
   ```

5. **Initialize database and seed data:**
   ```bash
   python seed_data.py
   ```
   
   This initializes DB tables only. Seed conversations are served from `web-app/backend/app/assets/annotated_test.json` at runtime.

6. **Run the backend:**
   ```bash
   uvicorn app.main:app --reload --host 0.0.0.0 --port 8080
   ```

### Frontend Setup

1. **Navigate to frontend directory:**
   ```bash
   cd web-app/frontend
   ```

2. **Install dependencies:**
   ```bash
   npm install
   ```

3. **Set environment variables:**
   Create a `.env` file:
   ```env
   REACT_APP_BACKEND_BASE_URL=http://localhost:8080
   ```

4. **Run the frontend:**
   ```bash
   npm start
   ```

The frontend will be available at `http://localhost:3000`.

### Docker Compose Setup (Recommended)

1. **Configure Docker Desktop (macOS):**
   To ensure containers continue running after quitting Docker Desktop:
   - Open Docker Desktop
   - Go to **Settings** (gear icon) → **General**
   - Enable **"Start Docker Desktop when you log in"** (optional but recommended)
   - Go to **Settings** → **Resources** → **Advanced**
   - Ensure Docker Desktop is set to keep the daemon running
   - **Important**: On macOS, Docker Desktop can be quit from the menu bar, but the Docker daemon will continue running in the background by default. Containers will keep running as long as the daemon is active.

2. **Set environment variables:**
   Create a `.env` file in the `web-app` directory:
   ```env
   GEMINI_API_KEY=your_gemini_api_key_here
   GEMINI_MODEL=gemini-3-flash-preview
   GEMINI_TIMEOUT_SECONDS=15
   GEMINI_MAX_ATTEMPTS=2
   GEMINI_LIVE_TIMEOUT_SECONDS=8
   GEMINI_LIVE_MAX_ATTEMPTS=1
   ```

3. **Start services in detached mode (runs in background):**
   ```bash
   cd web-app
   docker-compose up -d --build
   ```
   
   The `-d` flag runs containers in detached mode, so they continue running even after you close the terminal.

4. **Seed the database:**
   ```bash
   docker-compose exec web-app-backend python seed_data.py
   ```

5. **View logs (optional):**
   ```bash
   # View all logs
   docker-compose logs -f
   
   # View logs for specific service
   docker-compose logs -f web-app-backend
   docker-compose logs -f web-app-frontend
   ```

The backend will be available at `http://localhost:8080` and frontend at `http://localhost:3000`.

### Docker Container Management

**Start containers:**
```bash
cd web-app
docker-compose up -d
```

**Stop containers:**
```bash
cd web-app
docker-compose down
```

**Stop and remove volumes (clears database):**
```bash
docker-compose down -v
```

**Restart containers:**
```bash
docker-compose restart
```

**Check container status:**
```bash
docker-compose ps
```

**View running containers:**
```bash
docker ps
```

**Note**: Containers started with `docker-compose up -d` will continue running even if you:
- Close the terminal window
- Quit Docker Desktop GUI (on macOS, the daemon keeps running)
- Log out and log back in (if Docker Desktop is set to start on login)

To completely stop containers, use `docker-compose down` in the terminal.

## Database Seeding

Seed conversations are loaded at runtime from `web-app/backend/app/assets/annotated_test.json`:

```bash
cd web-app/backend
python seed_data.py
```

Or with Docker:
```bash
docker-compose exec web-app-backend python seed_data.py
```

## API Endpoints

### Participants
- `POST /api/participants` - Create participant (A/B assignment)
- `GET /api/participants/{id}` - Get participant

### Conversations
- `GET /api/conversations/seed` - Get all seed conversations
- `GET /api/conversations/seed/{id}` - Get specific conversation
- `POST /api/conversations/sessions/{participant_id}/{conversation_id}` - Create session
- `GET /api/conversations/sessions/{session_id}` - Get session

### Risk Assessment
- `POST /api/risk/assess` - Assess risk of draft message

### User Inputs
- `POST /api/user-inputs` - Capture user input
- `POST /api/user-inputs/with-warning` - Capture input with warning data

### Surveys
- `POST /api/surveys/responses` - Submit survey response
- `GET /api/surveys/responses/{participant_id}` - Get responses

### Completion
- `GET /api/completion/prolific` - Get Prolific completion URL
- `GET /api/completion/redirect` - Redirect to Prolific

## Security Features

- CORS configuration
- Environment-based secrets
- HTTPS-only cookies (in production)
- Input validation via Pydantic
- Security headers middleware

## Deployment

See [DEPLOYMENT.md](./DEPLOYMENT.md) for detailed deployment instructions.

## Testing Flow

1. User arrives at welcome screen
2. Participant is created and assigned to variant A or B
3. User goes through three conversation scenarios
4. After each conversation, user answers survey questions
5. User is redirected to Prolific completion URL

## Project Structure

```
web-app/
├── backend/              # FastAPI backend
│   ├── app/             # Application code
│   │   ├── routers/     # API route handlers
│   │   ├── services/    # Business logic
│   │   ├── models.py    # Database models
│   │   └── schemas.py   # Pydantic schemas
│   ├── Dockerfile
│   ├── requirements.txt
│   └── seed_data.py     # Database seeding script
├── frontend/             # React frontend
│   ├── src/
│   │   ├── components/  # React components
│   │   └── App.js       # Main app
│   └── package.json
├── docker-compose.yml   # Multi-service setup
├── README.md           # This file
└── DEPLOYMENT.md       # Deployment guide
```
