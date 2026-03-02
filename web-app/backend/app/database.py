"""
Database setup and session management.
Normalized schema with 7 tables.
"""
import logging
import os

from fastapi import HTTPException
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

# Prefer direct env lookup for startup resilience.
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()

# Create database engine
engine: Engine | None = None
if not DATABASE_URL:
    logger.warning("DATABASE_URL not set, DB features disabled.")
else:
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    except Exception:
        logger.exception("Failed to create DB engine; DB features disabled.")
        engine = None

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False)

# Base class for models
Base = declarative_base()


def is_db_configured() -> bool:
    """Return whether database engine is available."""
    return engine is not None


def require_db() -> Engine:
    """Require a configured DB engine for DB-backed request handlers."""
    if engine is None:
        raise HTTPException(status_code=503, detail="Database not configured")
    return engine


def get_db_dialect() -> str:
    """Return active SQL dialect name, e.g. postgresql."""
    if engine is not None:
        return engine.url.get_backend_name()
    if DATABASE_URL and "://" in DATABASE_URL:
        return DATABASE_URL.split("://", 1)[0]
    return "disabled"


def _ensure_participant_views() -> None:
    """
    Create convenience views that expose participant identifiers alongside
    normalized response rows.
    """
    view_defs = {
        "v_baseline_assessment": """
            SELECT
                ba.id,
                ba.participant_id,
                ba.recognize_sensitive,
                ba.avoid_accidental,
                ba.familiar_scams,
                ba.contextual_judgment,
                p.prolific_id AS participant_prolific_id,
                p.variant AS participant_variant
            FROM baseline_assessment ba
            JOIN participants p ON p.id = ba.participant_id
        """,
        "v_scenario_responses": """
            SELECT
                sr.id,
                sr.participant_id,
                sr.scenario_number,
                sr.original_input,
                sr.masked_text,
                sr.risk_level,
                sr."Reasoning",
                sr.suggested_rewrite,
                sr.final_message,
                sr.primary_risk_factors,
                sr.linkability_risk_level,
                sr.linkability_risk_explanation,
                sr.authentication_baiting_level,
                sr.authentication_baiting_explanation,
                sr.contextual_alignment_level,
                sr.contextual_alignment_explanation,
                sr.platform_trust_obligation_level,
                sr.platform_trust_obligation_explanation,
                sr.psychological_pressure_level,
                sr.psychological_pressure_explanation,
                sr.accepted_rewrite,
                sr.completed_at,
                p.prolific_id AS participant_prolific_id,
                p.variant AS participant_variant
            FROM scenario_responses sr
            JOIN participants p ON p.id = sr.participant_id
        """,
        "v_post_scenario_survey": """
            SELECT
                pss.id,
                pss.participant_id,
                pss.scenario_number,
                pss.confidence_judgment,
                pss.uncertainty_sharing,
                pss.perceived_risk,
                pss.warning_clarity,
                pss.warning_helpful,
                pss.rewrite_quality,
                p.prolific_id AS participant_prolific_id,
                p.variant AS participant_variant
            FROM post_scenario_survey pss
            JOIN participants p ON p.id = pss.participant_id
        """,
        "v_pii_disclosure": """
            SELECT
                pd.id,
                pd.participant_id,
                pd.scenario_number,
                pd.pii_type,
                pd.other_specified,
                p.prolific_id AS participant_prolific_id,
                p.variant AS participant_variant
            FROM pii_disclosure pd
            JOIN participants p ON p.id = pd.participant_id
        """,
        "v_sus_responses": """
            SELECT
                sus.id,
                sus.participant_id,
                sus.sus_1,
                sus.sus_2,
                sus.sus_3,
                sus.sus_4,
                sus.sus_5,
                sus.sus_6,
                sus.sus_7,
                sus.sus_8,
                sus.sus_9,
                sus.sus_10,
                sus.sus_score,
                p.prolific_id AS participant_prolific_id,
                p.variant AS participant_variant
            FROM sus_responses sus
            JOIN participants p ON p.id = sus.participant_id
        """,
        "v_end_of_study_survey": """
            SELECT
                eos.id,
                eos.participant_id,
                eos.tasks_realistic,
                eos.realism_explanation,
                eos.overall_confidence,
                eos.sharing_rationale,
                eos.trust_system,
                eos.trust_explanation,
                p.prolific_id AS participant_prolific_id,
                p.variant AS participant_variant
            FROM end_of_study_survey eos
            JOIN participants p ON p.id = eos.participant_id
        """,
    }
    db_engine = require_db()
    with db_engine.begin() as conn:
        for view_name, select_sql in view_defs.items():
            conn.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
            conn.execute(text(f"CREATE VIEW {view_name} AS {select_sql}"))


def get_db():
    """Dependency to get database session."""
    db_engine = require_db()
    db = SessionLocal(bind=db_engine)
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    """
    Initialize database tables.
    Creates the normalized tables:
    1. consent_decisions
    2. participants
    3. baseline_assessment
    4. scenario_responses
    5. post_scenario_survey
    6. pii_disclosure
    7. sus_responses
    8. end_of_study_survey
    """
    if engine is None:
        logger.warning("Skipping DB initialization because database is not configured.")
        return
    Base.metadata.create_all(bind=engine)
    _ensure_participant_views()
    logger.info("Database tables initialized")


def reset_db() -> None:
    """
    Drop all tables and recreate them.
    WARNING: This will delete all data!
    """
    db_engine = require_db()
    Base.metadata.drop_all(bind=db_engine)
    Base.metadata.create_all(bind=db_engine)
    logger.info("Database reset - all tables dropped and recreated")


def get_table_info() -> dict:
    """Get information about all tables in the database (for debugging)."""
    db_engine = require_db()
    tables = {}
    try:
        inspector = inspect(db_engine)
        with db_engine.connect() as conn:
            for table_name in inspector.get_table_names():
                columns_meta = inspector.get_columns(table_name)
                columns = [
                    {
                        "name": col.get("name"),
                        "type": str(col.get("type")),
                        "nullable": bool(col.get("nullable", True)),
                    }
                    for col in columns_meta
                ]
                count = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar_one()
                tables[table_name] = {"columns": columns, "row_count": count}
    except Exception as e:
        logger.error(f"Error getting table info: {e}")

    return tables
