"""
Database setup and session management.
Normalized schema with 7 tables.
"""
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app.config import settings

logger = logging.getLogger(__name__)

# Create database engine
if settings.DATABASE_URL.startswith("sqlite"):
    engine = create_engine(
        settings.DATABASE_URL,
        connect_args={"check_same_thread": False}  # SQLite-specific
    )
else:
    engine = create_engine(settings.DATABASE_URL)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for models
Base = declarative_base()


def _ensure_sqlite_column(table_name: str, column_name: str, column_sql_type: str) -> None:
    """Best-effort schema patch for existing SQLite DBs without migrations."""
    with engine.begin() as conn:
        info_rows = conn.execute(text(f"PRAGMA table_info('{table_name}')")).fetchall()
        existing = {row[1] for row in info_rows}
        if column_name in existing:
            return
        conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql_type}"))


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
    with engine.begin() as conn:
        for view_name, select_sql in view_defs.items():
            conn.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
            conn.execute(text(f"CREATE VIEW {view_name} AS {select_sql}"))


def _sqlite_column_names(table_name: str) -> set[str]:
    """Return SQLite column names for a table."""
    with engine.begin() as conn:
        info_rows = conn.execute(text(f"PRAGMA table_info('{table_name}')")).fetchall()
    return {row[1] for row in info_rows}


def _sqlite_column_order(table_name: str) -> list[str]:
    """Return SQLite column names in table order."""
    with engine.begin() as conn:
        info_rows = conn.execute(text(f"PRAGMA table_info('{table_name}')")).fetchall()
    return [row[1] for row in info_rows]


def _scenario_layout_requires_rebuild(columns_in_order: list[str]) -> bool:
    """
    Ensure key scenario fields follow the required order:
    ... masked_text, risk_level, Reasoning, suggested_rewrite, final_message ...
    """
    required_sequence = ["masked_text", "risk_level", "Reasoning", "suggested_rewrite", "final_message"]
    positions = []
    for col in required_sequence:
        if col not in columns_in_order:
            return True
        positions.append(columns_in_order.index(col))
    if positions != sorted(positions):
        return True
    start = positions[0]
    return positions != list(range(start, start + len(required_sequence)))


def _backfill_scenario_response_columns() -> None:
    """Copy legacy scenario columns into renamed columns when present."""
    columns = _sqlite_column_names("scenario_responses")
    with engine.begin() as conn:
        if "original_input" in columns and "user_input" in columns:
            conn.execute(text(
                "UPDATE scenario_responses "
                "SET original_input = user_input "
                "WHERE original_input IS NULL AND user_input IS NOT NULL"
            ))
        if "Reasoning" in columns and "reasoning_steps" in columns:
            conn.execute(text(
                "UPDATE scenario_responses "
                "SET \"Reasoning\" = reasoning_steps "
                "WHERE \"Reasoning\" IS NULL AND reasoning_steps IS NOT NULL"
            ))
        if "linkability_risk_level" in columns and "pii_sensitivity_level" in columns:
            conn.execute(text(
                "UPDATE scenario_responses "
                "SET linkability_risk_level = pii_sensitivity_level "
                "WHERE linkability_risk_level IS NULL AND pii_sensitivity_level IS NOT NULL"
            ))
        if "linkability_risk_explanation" in columns and "pii_sensitivity_explanation" in columns:
            conn.execute(text(
                "UPDATE scenario_responses "
                "SET linkability_risk_explanation = pii_sensitivity_explanation "
                "WHERE linkability_risk_explanation IS NULL AND pii_sensitivity_explanation IS NOT NULL"
            ))
        if "contextual_alignment_level" in columns and "contextual_necessity_level" in columns:
            conn.execute(text(
                "UPDATE scenario_responses "
                "SET contextual_alignment_level = contextual_necessity_level "
                "WHERE contextual_alignment_level IS NULL AND contextual_necessity_level IS NOT NULL"
            ))
        if "contextual_alignment_explanation" in columns and "contextual_necessity_explanation" in columns:
            conn.execute(text(
                "UPDATE scenario_responses "
                "SET contextual_alignment_explanation = contextual_necessity_explanation "
                "WHERE contextual_alignment_explanation IS NULL AND contextual_necessity_explanation IS NOT NULL"
            ))
        if "platform_trust_obligation_level" in columns and "intent_trajectory_level" in columns:
            conn.execute(text(
                "UPDATE scenario_responses "
                "SET platform_trust_obligation_level = intent_trajectory_level "
                "WHERE platform_trust_obligation_level IS NULL AND intent_trajectory_level IS NOT NULL"
            ))
        if "platform_trust_obligation_explanation" in columns and "intent_trajectory_explanation" in columns:
            conn.execute(text(
                "UPDATE scenario_responses "
                "SET platform_trust_obligation_explanation = intent_trajectory_explanation "
                "WHERE platform_trust_obligation_explanation IS NULL AND intent_trajectory_explanation IS NOT NULL"
            ))


def _backfill_scenario_completed_at_from_participant() -> None:
    """Populate scenario completed_at from participant completed_at when missing."""
    columns = _sqlite_column_names("scenario_responses")
    if "completed_at" not in columns:
        return
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE scenario_responses
            SET completed_at = (
                SELECT p.completed_at
                FROM participants p
                WHERE p.id = scenario_responses.participant_id
            )
            WHERE completed_at IS NULL
              AND (
                SELECT p.completed_at
                FROM participants p
                WHERE p.id = scenario_responses.participant_id
              ) IS NOT NULL
        """))
        # Normalize to SQLite's default second-precision timestamp text format.
        conn.execute(text("""
            UPDATE scenario_responses
            SET completed_at = substr(completed_at, 1, 19)
            WHERE completed_at IS NOT NULL
              AND instr(completed_at, '.') > 0
        """))


def _rebuild_scenario_responses_if_legacy() -> None:
    """Drop deprecated columns by rebuilding scenario_responses with the canonical schema."""
    columns = _sqlite_column_names("scenario_responses")
    column_order = _sqlite_column_order("scenario_responses")
    deprecated_columns = {
        "user_input",
        "rewrite",
        "risk_explanation",
        "reasoning_steps",
        "Explanation_NIST",
        "pii_sensitivity_level",
        "pii_sensitivity_explanation",
        "contextual_necessity_level",
        "contextual_necessity_explanation",
        "intent_trajectory_level",
        "intent_trajectory_explanation",
        "identity_trust_signals_flags",
        "identity_trust_signals_explanation",
    }
    needs_order_rebuild = _scenario_layout_requires_rebuild(column_order)
    if not (columns & deprecated_columns) and not needs_order_rebuild:
        return

    # Ensure all referenced columns exist before the data-copy SELECT.
    for col in [
        "original_input",
        "Reasoning",
        "linkability_risk_level",
        "linkability_risk_explanation",
        "authentication_baiting_level",
        "authentication_baiting_explanation",
        "contextual_alignment_level",
        "contextual_alignment_explanation",
        "platform_trust_obligation_level",
        "platform_trust_obligation_explanation",
        "psychological_pressure_level",
        "psychological_pressure_explanation",
        "pii_sensitivity_level",
        "pii_sensitivity_explanation",
        "contextual_necessity_level",
        "contextual_necessity_explanation",
        "intent_trajectory_level",
        "intent_trajectory_explanation",
    ]:
        if col not in columns:
            _ensure_sqlite_column("scenario_responses", col, "TEXT")
    for col in deprecated_columns:
        if col not in columns:
            _ensure_sqlite_column("scenario_responses", col, "TEXT")

    logger.info(
        "Rebuilding scenario_responses (drop deprecated=%s, fix_order=%s)",
        sorted(columns & deprecated_columns),
        needs_order_rebuild,
    )
    with engine.begin() as conn:
        # The old view depends on scenario_responses and blocks table swap in SQLite.
        conn.execute(text("DROP VIEW IF EXISTS v_scenario_responses"))
        conn.execute(text("DROP TABLE IF EXISTS scenario_responses_new"))
        conn.execute(text("""
            CREATE TABLE scenario_responses_new (
                id INTEGER PRIMARY KEY,
                participant_id INTEGER NOT NULL REFERENCES participants(id) ON DELETE CASCADE,
                scenario_number INTEGER NOT NULL,
                original_input TEXT NULL,
                masked_text TEXT NULL,
                risk_level VARCHAR NULL,
                "Reasoning" TEXT NULL,
                suggested_rewrite TEXT NULL,
                final_message TEXT NULL,
                primary_risk_factors TEXT NULL,
                linkability_risk_level VARCHAR NULL,
                linkability_risk_explanation TEXT NULL,
                authentication_baiting_level VARCHAR NULL,
                authentication_baiting_explanation TEXT NULL,
                contextual_alignment_level VARCHAR NULL,
                contextual_alignment_explanation TEXT NULL,
                platform_trust_obligation_level VARCHAR NULL,
                platform_trust_obligation_explanation TEXT NULL,
                psychological_pressure_level VARCHAR NULL,
                psychological_pressure_explanation TEXT NULL,
                accepted_rewrite BOOLEAN NULL,
                started_at DATETIME NULL,
                completed_at DATETIME NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_scenario_response_participant_scenario UNIQUE(participant_id, scenario_number)
            )
        """))
        conn.execute(text("""
            INSERT INTO scenario_responses_new (
                id, participant_id, scenario_number, original_input, masked_text, risk_level,
                "Reasoning", suggested_rewrite, final_message, primary_risk_factors,
                linkability_risk_level, linkability_risk_explanation,
                authentication_baiting_level, authentication_baiting_explanation,
                contextual_alignment_level, contextual_alignment_explanation,
                platform_trust_obligation_level, platform_trust_obligation_explanation,
                psychological_pressure_level, psychological_pressure_explanation,
                accepted_rewrite, started_at, completed_at, created_at
            )
            SELECT
                id,
                participant_id,
                scenario_number,
                COALESCE(original_input, user_input),
                masked_text,
                risk_level,
                COALESCE("Reasoning", reasoning_steps),
                COALESCE(suggested_rewrite, rewrite),
                final_message,
                primary_risk_factors,
                COALESCE(linkability_risk_level, pii_sensitivity_level),
                COALESCE(linkability_risk_explanation, pii_sensitivity_explanation),
                authentication_baiting_level,
                authentication_baiting_explanation,
                COALESCE(contextual_alignment_level, contextual_necessity_level),
                COALESCE(contextual_alignment_explanation, contextual_necessity_explanation),
                COALESCE(platform_trust_obligation_level, intent_trajectory_level),
                COALESCE(platform_trust_obligation_explanation, intent_trajectory_explanation),
                psychological_pressure_level,
                psychological_pressure_explanation,
                accepted_rewrite,
                started_at,
                completed_at,
                created_at
            FROM scenario_responses
        """))
        conn.execute(text("DROP TABLE scenario_responses"))
        conn.execute(text("ALTER TABLE scenario_responses_new RENAME TO scenario_responses"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_scenario_responses_id ON scenario_responses (id)"))


def get_db():
    """Dependency to get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
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
    Base.metadata.create_all(bind=engine)
    # Lightweight in-place migration for SQLite to keep old local DBs compatible.
    if settings.DATABASE_URL.startswith("sqlite"):
        _rebuild_scenario_responses_if_legacy()
        scenario_columns = [
            ("original_input", "TEXT"),
            ("Reasoning", "TEXT"),
            ("risk_level", "TEXT"),
            ("primary_risk_factors", "TEXT"),
            ("linkability_risk_level", "TEXT"),
            ("linkability_risk_explanation", "TEXT"),
            ("authentication_baiting_level", "TEXT"),
            ("authentication_baiting_explanation", "TEXT"),
            ("contextual_alignment_level", "TEXT"),
            ("contextual_alignment_explanation", "TEXT"),
            ("platform_trust_obligation_level", "TEXT"),
            ("platform_trust_obligation_explanation", "TEXT"),
            ("psychological_pressure_level", "TEXT"),
            ("psychological_pressure_explanation", "TEXT"),
        ]
        for column_name, column_type in scenario_columns:
            _ensure_sqlite_column("scenario_responses", column_name, column_type)
        _backfill_scenario_response_columns()
        _backfill_scenario_completed_at_from_participant()
    _ensure_participant_views()
    logger.info("Database tables initialized")


def reset_db():
    """
    Drop all tables and recreate them.
    WARNING: This will delete all data!
    """
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    logger.info("Database reset - all tables dropped and recreated")


def get_table_info():
    """Get information about all tables in the database (for debugging)."""
    if not settings.DATABASE_URL.startswith("sqlite"):
        return {}
    
    tables = {}
    try:
        with engine.connect() as conn:
            # Get list of tables
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
            table_names = [row[0] for row in result]
            
            for table_name in table_names:
                result = conn.execute(text(f"PRAGMA table_info('{table_name}')"))
                columns = [{"name": row[1], "type": row[2], "nullable": not row[3]} for row in result]
                
                result = conn.execute(text(f"SELECT COUNT(*) FROM '{table_name}'"))
                count = result.fetchone()[0]
                
                tables[table_name] = {"columns": columns, "row_count": count}
    except Exception as e:
        logger.error(f"Error getting table info: {e}")
    
    return tables
