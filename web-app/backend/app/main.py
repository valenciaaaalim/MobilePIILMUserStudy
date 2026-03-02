"""
Main FastAPI application for the web app backend.
"""
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import logging
import threading

from app.config import settings
from app.database import init_db, get_table_info, get_db_dialect, is_db_configured, require_db
from app.middleware.security import SecurityHeadersMiddleware
from app.routers import (
    participants,
    risk_assessment,
    participant_data,
    pii,
    consent,
    completion
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="WhatsApp Risk Assessment Web App",
    description="Web application for user testing of WhatsApp risk assessment",
    version="2.0.0"
)

# Security headers middleware
app.add_middleware(SecurityHeadersMiddleware)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_origin_regex=settings.ALLOWED_ORIGIN_REGEX,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    max_age=3600,
)

# Include routers
app.include_router(participants.router)
app.include_router(risk_assessment.router)
app.include_router(participant_data.router)
app.include_router(pii.router)
app.include_router(consent.router)
app.include_router(completion.router)


@app.on_event("startup")
async def startup_event():
    """Initialize database and other startup tasks."""
    logger.info("Starting up application (DATABASE_URL set=%s, db_dialect=%s)", bool(settings.DATABASE_URL), get_db_dialect())
    try:
        if is_db_configured():
            init_db()
            logger.info("Database initialized with 8 normalized tables")

            # Log table info for debugging
            table_info = get_table_info()
            for table_name, info in table_info.items():
                logger.info(f"  - {table_name}: {info['row_count']} rows, {len(info['columns'])} columns")
        else:
            logger.warning("Database is not configured at startup; DB-backed endpoints will return 503.")
        
        # Warm up GLiNER model in background
        def warm_pii_model():
            try:
                pii.get_gliner_service()
                logger.info("GLiNER model warmup completed")
            except Exception as e:
                logger.error(f"GLiNER warmup failed: {e}")
        threading.Thread(target=warm_pii_model, daemon=True).start()
    except Exception:
        logger.exception("Database initialization failed during startup; continuing to serve non-DB endpoints")


@app.get("/healthz")
async def healthz():
    """Liveness endpoint that does not touch the database."""
    return {"ok": True}


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "version": "2.0.0"
    }


@app.get("/db-info")
async def db_info():
    """Get database table information (for debugging)."""
    require_db()
    return get_table_info()


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.HOST, port=settings.PORT)
