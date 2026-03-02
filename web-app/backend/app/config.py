"""
Configuration settings for the web app backend.
"""
import os
from typing import List, Optional


def _clean_env(value: Optional[str]) -> Optional[str]:
    """Normalize env strings and strip accidental wrapping quotes."""
    if value is None:
        return None
    normalized = value.strip()
    if len(normalized) >= 2:
        if (normalized[0] == '"' and normalized[-1] == '"') or (
            normalized[0] == "'" and normalized[-1] == "'"
        ):
            normalized = normalized[1:-1].strip()
    return normalized


def _env_bool(name: str, default: bool) -> bool:
    """Parse env bool flags."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


class Settings:
    """Application settings."""
    
    # Database
    DATABASE_URL: Optional[str] = _clean_env(os.getenv("DATABASE_URL"))
    
    # CORS settings
    ALLOWED_ORIGINS: List[str] = [
        os.getenv("FRONTEND_URL", "http://localhost:3000"),
        "http://localhost:3000",
        "https://localhost:3000",
    ]
    ALLOWED_ORIGIN_REGEX: Optional[str] = os.getenv(
        "ALLOWED_ORIGIN_REGEX",
        r"http://localhost(:\d+)?|http://127\.0\.0\.1(:\d+)?"
    )
    
    # Gemini API
    # Per Google docs, GOOGLE_API_KEY is a supported standard env var.
    # If both are set, prefer GOOGLE_API_KEY.
    GOOGLE_API_KEY: Optional[str] = _clean_env(os.getenv("GOOGLE_API_KEY"))
    GEMINI_API_KEY: Optional[str] = _clean_env(os.getenv("GEMINI_API_KEY"))
    GEMINI_MODEL_NAME: str = (
        _clean_env(os.getenv("GEMINI_MODEL_NAME"))
        or "gemini-3-flash-preview"
    )
    GEMINI_TIMEOUT_SECONDS: int = int(os.getenv("GEMINI_TIMEOUT_SECONDS", "15"))
    GEMINI_MAX_ATTEMPTS: int = int(os.getenv("GEMINI_MAX_ATTEMPTS", "2"))
    GEMINI_LIVE_TIMEOUT_SECONDS: int = int(os.getenv("GEMINI_LIVE_TIMEOUT_SECONDS", "8"))
    GEMINI_LIVE_MAX_ATTEMPTS: int = int(os.getenv("GEMINI_LIVE_MAX_ATTEMPTS", "1"))
    GEMINI_THINKING_LEVEL: str = (_clean_env(os.getenv("GEMINI_THINKING_LEVEL")) or "low").lower()
    GEMINI_INCLUDE_THOUGHTS: bool = _env_bool("GEMINI_INCLUDE_THOUGHTS", True)
    
    # Security
    SECRET_KEY: str = os.getenv("SECRET_KEY", "change-me-in-production")
    CSRF_SECRET: str = os.getenv("CSRF_SECRET", "change-me-in-production")
    SESSION_COOKIE_NAME: str = "session_id"
    SESSION_COOKIE_SECURE: bool = os.getenv("SESSION_COOKIE_SECURE", "false").lower() == "true"
    SESSION_COOKIE_HTTPONLY: bool = True
    SESSION_COOKIE_SAMESITE: str = "lax"
    
    # Server settings
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8000"))
    
    # Cloud Run settings
    PROJECT_ID: str = os.getenv("GOOGLE_CLOUD_PROJECT", "")
    REGION: str = os.getenv("REGION", "us-central1")
    SERVICE_NAME: str = os.getenv("SERVICE_NAME", "web-app-backend")
    
    # Prolific
    PROLIFIC_COMPLETION_URL: str = os.getenv(
        "PROLIFIC_COMPLETION_URL",
        "https://app.prolific.co/submissions/complete"
    )
    
    # Rate limiting
    RATE_LIMIT_PER_MINUTE: int = int(os.getenv("RATE_LIMIT_PER_MINUTE", "60"))


settings = Settings()
