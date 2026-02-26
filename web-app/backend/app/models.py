"""
Database models for the web app.
Normalized schema with 7 tables for user study data.
"""
from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, Float, UniqueConstraint, Enum as SQLEnum, Numeric
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base
from app.utils import get_singapore_time
import enum


class VariantEnum(str, enum.Enum):
    A = "A"
    B = "B"


class PiiTypeEnum(str, enum.Enum):
    phone_number = "phone_number"
    home_address = "home_address"
    email_address = "email_address"
    date_of_birth = "date_of_birth"
    workplace = "workplace"
    government_id = "government_id"
    financial_info = "financial_info"
    none_disclosed = "none_disclosed"
    other = "other"


# =============================================================================
# TABLE 1: participants - Core table
# =============================================================================
class Participant(Base):
    """Participant/user model - Core table for the study."""
    __tablename__ = "participants"
    
    id = Column(Integer, primary_key=True, index=True)
    prolific_id = Column(String, unique=True, index=True, nullable=True)
    variant = Column(String, nullable=False)  # 'A' or 'B' (stored as string for SQLite compatibility)
    created_at = Column(DateTime(timezone=True), default=get_singapore_time)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    duration_seconds = Column(Float, nullable=True)  # Duration of study in seconds
    is_complete = Column(Boolean, default=False)  # Whether participant completed the study
    
    # Relationships to normalized tables
    baseline_assessment = relationship("BaselineAssessment", back_populates="participant", uselist=False, cascade="all, delete-orphan")
    scenario_responses = relationship("ScenarioResponse", back_populates="participant", cascade="all, delete-orphan")
    post_scenario_surveys = relationship("PostScenarioSurvey", back_populates="participant", cascade="all, delete-orphan")
    pii_disclosures = relationship("PiiDisclosure", back_populates="participant", cascade="all, delete-orphan")
    sus_responses = relationship("SusResponse", back_populates="participant", uselist=False, cascade="all, delete-orphan")
    end_of_study_survey = relationship("EndOfStudySurvey", back_populates="participant", uselist=False, cascade="all, delete-orphan")


# =============================================================================
# TABLE 0: consent_decisions - Consent logging
# =============================================================================
class ConsentDecision(Base):
    """Consent decision log - yes/no with UTC timestamp."""
    __tablename__ = "consent_decisions"

    id = Column(Integer, primary_key=True, index=True)
    participant_platform_id = Column(String, nullable=True, index=True)
    consent = Column(String, nullable=False)  # 'yes' or 'no'
    timestamp_utc = Column(DateTime(timezone=True), nullable=False)


# =============================================================================
# TABLE 2: baseline_assessment - 4 Likert (1-7) responses
# =============================================================================
class BaselineAssessment(Base):
    """Baseline Self-Assessment - 4 Likert (1-7) responses."""
    __tablename__ = "baseline_assessment"
    
    id = Column(Integer, primary_key=True, index=True)
    participant_id = Column(Integer, ForeignKey("participants.id", ondelete="CASCADE"), nullable=False, unique=True)
    recognize_sensitive = Column(Integer, nullable=False)  # Likert 1-7
    avoid_accidental = Column(Integer, nullable=False)  # Likert 1-7
    familiar_scams = Column(Integer, nullable=False)  # Likert 1-7
    contextual_judgment = Column(Integer, nullable=False)  # Likert 1-7
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    participant = relationship("Participant", back_populates="baseline_assessment")


# =============================================================================
# TABLE 3: scenario_responses - Per-scenario user input/response data
# =============================================================================
class ScenarioResponse(Base):
    """Scenario responses - stores per-scenario (1-3) user input and system data."""
    __tablename__ = "scenario_responses"
    __table_args__ = (
        UniqueConstraint("participant_id", "scenario_number", name="uq_scenario_response_participant_scenario"),
    )
    
    id = Column(Integer, primary_key=True, index=True)
    participant_id = Column(Integer, ForeignKey("participants.id", ondelete="CASCADE"), nullable=False)
    scenario_number = Column(Integer, nullable=False)  # 1, 2, or 3
    original_input = Column(Text, nullable=True)  # Text sent to LLM for assessment
    masked_text = Column(Text, nullable=True)  # PII-masked version
    risk_level = Column(String, nullable=True)  # Output_2 Risk_Level
    reasoning = Column("Reasoning", Text, nullable=True)  # Output_2 Reasoning
    suggested_rewrite = Column(Text, nullable=True)  # Suggested safer rewrite from LLM
    final_message = Column(Text, nullable=True)  # Final message sent by user
    primary_risk_factors = Column(Text, nullable=True)  # JSON array from Output_2 Primary_Risk_Factors
    linkability_risk_level = Column(String, nullable=True)
    linkability_risk_explanation = Column(Text, nullable=True)
    authentication_baiting_level = Column(String, nullable=True)
    authentication_baiting_explanation = Column(Text, nullable=True)
    contextual_alignment_level = Column(String, nullable=True)
    contextual_alignment_explanation = Column(Text, nullable=True)
    platform_trust_obligation_level = Column(String, nullable=True)
    platform_trust_obligation_explanation = Column(Text, nullable=True)
    psychological_pressure_level = Column(String, nullable=True)
    psychological_pressure_explanation = Column(Text, nullable=True)
    accepted_rewrite = Column(Boolean, nullable=True)  # Whether user accepted the rewrite
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    participant = relationship("Participant", back_populates="scenario_responses")


# =============================================================================
# TABLE 4: post_scenario_survey - Per-scenario survey responses
# =============================================================================
class PostScenarioSurvey(Base):
    """Post-Scenario Survey - per scenario (1-3)."""
    __tablename__ = "post_scenario_survey"
    __table_args__ = (
        UniqueConstraint("participant_id", "scenario_number", name="uq_post_scenario_participant_scenario"),
    )
    
    id = Column(Integer, primary_key=True, index=True)
    participant_id = Column(Integer, ForeignKey("participants.id", ondelete="CASCADE"), nullable=False)
    scenario_number = Column(Integer, nullable=False)  # 1, 2, or 3
    # Common questions (both groups) - Likert 1-7
    confidence_judgment = Column(Integer, nullable=False)
    uncertainty_sharing = Column(Integer, nullable=False)
    perceived_risk = Column(Integer, nullable=False)
    # Group A only - Likert 1-7 (nullable)
    warning_clarity = Column(Integer, nullable=True)
    warning_helpful = Column(Integer, nullable=True)
    rewrite_quality = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    participant = relationship("Participant", back_populates="post_scenario_surveys")


# =============================================================================
# TABLE 5: pii_disclosure - Normalized checkbox responses (one row per selection)
# =============================================================================
class PiiDisclosure(Base):
    """PII Disclosure - normalized table for checkbox responses. Each row = one PII type selected."""
    __tablename__ = "pii_disclosure"
    
    id = Column(Integer, primary_key=True, index=True)
    participant_id = Column(Integer, ForeignKey("participants.id", ondelete="CASCADE"), nullable=False)
    scenario_number = Column(Integer, nullable=False)  # 1, 2, or 3
    pii_type = Column(String, nullable=False)  # ENUM values as string for SQLite
    other_specified = Column(String, nullable=True)  # Text if pii_type is 'other'
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    participant = relationship("Participant", back_populates="pii_disclosures")


# =============================================================================
# TABLE 6: sus_responses - Group A only, 10 SUS items (1-5) + calculated score
# =============================================================================
class SusResponse(Base):
    """SUS Responses - Group A only. 10 SUS items (1-5 scale) plus calculated SUS score."""
    __tablename__ = "sus_responses"
    
    id = Column(Integer, primary_key=True, index=True)
    participant_id = Column(Integer, ForeignKey("participants.id", ondelete="CASCADE"), nullable=False, unique=True)
    sus_1 = Column(Integer, nullable=False)  # 1-5
    sus_2 = Column(Integer, nullable=False)  # 1-5
    sus_3 = Column(Integer, nullable=False)  # 1-5
    sus_4 = Column(Integer, nullable=False)  # 1-5
    sus_5 = Column(Integer, nullable=False)  # 1-5
    sus_6 = Column(Integer, nullable=False)  # 1-5
    sus_7 = Column(Integer, nullable=False)  # 1-5
    sus_8 = Column(Integer, nullable=False)  # 1-5
    sus_9 = Column(Integer, nullable=False)  # 1-5
    sus_10 = Column(Integer, nullable=False)  # 1-5
    sus_score = Column(Numeric(5, 2), nullable=True)  # Calculated SUS score (0-100)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    participant = relationship("Participant", back_populates="sus_responses")


# =============================================================================
# TABLE 7: end_of_study_survey - Both groups with Group A specific fields
# =============================================================================
class EndOfStudySurvey(Base):
    """End-of-Study Survey - both groups with Group A specific fields."""
    __tablename__ = "end_of_study_survey"
    
    id = Column(Integer, primary_key=True, index=True)
    participant_id = Column(Integer, ForeignKey("participants.id", ondelete="CASCADE"), nullable=False, unique=True)
    # Common questions (both groups)
    tasks_realistic = Column(Integer, nullable=False)  # Likert 1-7
    realism_explanation = Column(Text, nullable=True)  # Free text
    overall_confidence = Column(Integer, nullable=False)  # Likert 1-7
    sharing_rationale = Column(Text, nullable=True)  # Free text
    # Group A only (nullable)
    trust_system = Column(Integer, nullable=True)  # Likert 1-7
    trust_explanation = Column(Text, nullable=True)  # Free text
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    participant = relationship("Participant", back_populates="end_of_study_survey")
