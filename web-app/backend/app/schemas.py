"""
Pydantic schemas for API requests and responses.
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Union
from datetime import datetime


# =============================================================================
# Risk Assessment schemas
# =============================================================================
class MessageSchema(BaseModel):
    """Message schema for conversation history."""
    id: str
    text: str
    direction: str  # 'SENT' or 'RECEIVED'
    name: Optional[str] = None
    timestamp: Optional[datetime] = None


class RiskAssessmentRequest(BaseModel):
    """Risk assessment request."""
    draft_text: str = Field(..., min_length=1)
    masked_text: Optional[str] = None  # Pre-masked text from frontend PII detection
    masked_history: Optional[List[MessageSchema]] = None
    conversation_history: List[MessageSchema] = Field(default_factory=list)
    session_id: int  # Scenario number (1, 2, or 3)
    participant_prolific_id: Optional[str] = None


class RiskAssessmentResponse(BaseModel):
    """Risk assessment response."""
    risk_level: str  # 'LOW', 'MODERATE', 'HIGH'
    safer_rewrite: str
    show_warning: bool
    primary_risk_factors: List[str] = Field(default_factory=list)
    reasoning: Optional[str] = None
    output_1: Optional[Dict[str, Any]] = None
    output_2: Optional[Dict[str, Any]] = None


# =============================================================================
# Participant schemas
# =============================================================================
class ParticipantCreate(BaseModel):
    """Participant creation."""
    prolific_id: Optional[str] = None


class ParticipantSchema(BaseModel):
    """Participant schema."""
    id: int
    prolific_id: Optional[str]
    variant: str  # 'A' or 'B'
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    is_complete: Optional[bool] = None

    class Config:
        from_attributes = True


class ParticipantCreateResponse(BaseModel):
    """Participant creation response."""
    id: int
    prolific_id: Optional[str]
    variant: str
    status: str  # 'new', 'existing', 'completed'
    completion_url: Optional[str] = None


# =============================================================================
# Consent schemas
# =============================================================================
class ConsentDecisionCreate(BaseModel):
    """Consent decision log."""
    consent: str = Field(..., pattern="^(yes|no)$")
    participant_platform_id: Optional[str] = None


class ConsentDecisionResponse(BaseModel):
    """Consent decision response."""
    status: str


# =============================================================================
# Baseline Assessment schemas (Table 2)
# =============================================================================
class BaselineAssessmentCreate(BaseModel):
    """Baseline Self-Assessment - 4 Likert (1-7) responses."""
    recognize_sensitive: int = Field(..., ge=1, le=7)
    avoid_accidental: int = Field(..., ge=1, le=7)
    familiar_scams: int = Field(..., ge=1, le=7)
    contextual_judgment: int = Field(..., ge=1, le=7)


class BaselineAssessmentResponse(BaseModel):
    """Baseline Assessment response."""
    id: int
    participant_id: int
    recognize_sensitive: int
    avoid_accidental: int
    familiar_scams: int
    contextual_judgment: int
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# =============================================================================
# Scenario Response schemas (Table 3)
# =============================================================================
class ScenarioResponseCreate(BaseModel):
    """Scenario response creation - for recording message data."""
    scenario_number: int = Field(..., ge=1, le=3)
    original_input: Optional[str] = None  # Text sent to LLM for assessment
    masked_text: Optional[str] = None  # PII-masked version
    model: Optional[str] = None  # Actual model used for rewrite generation
    suggested_rewrite: Optional[str] = None  # LLM suggested rewrite
    reasoning: Optional[str] = None
    risk_level: Optional[str] = None
    primary_risk_factors: Optional[str] = None
    linkability_risk_level: Optional[str] = None
    linkability_risk_explanation: Optional[str] = None
    authentication_baiting_level: Optional[str] = None
    authentication_baiting_explanation: Optional[str] = None
    contextual_alignment_level: Optional[str] = None
    contextual_alignment_explanation: Optional[str] = None
    platform_trust_obligation_level: Optional[str] = None
    platform_trust_obligation_explanation: Optional[str] = None
    psychological_pressure_level: Optional[str] = None
    psychological_pressure_explanation: Optional[str] = None
    final_message: Optional[str] = None  # Final sent message
    accepted_rewrite: Optional[Union[bool, str]] = None  # "true" | "false" | null | "[B]"


class ScenarioResponseUpdate(BaseModel):
    """Scenario response update - for updating existing response."""
    original_input: Optional[str] = None
    masked_text: Optional[str] = None
    model: Optional[str] = None
    suggested_rewrite: Optional[str] = None
    reasoning: Optional[str] = None
    risk_level: Optional[str] = None
    primary_risk_factors: Optional[str] = None
    linkability_risk_level: Optional[str] = None
    linkability_risk_explanation: Optional[str] = None
    authentication_baiting_level: Optional[str] = None
    authentication_baiting_explanation: Optional[str] = None
    contextual_alignment_level: Optional[str] = None
    contextual_alignment_explanation: Optional[str] = None
    platform_trust_obligation_level: Optional[str] = None
    platform_trust_obligation_explanation: Optional[str] = None
    psychological_pressure_level: Optional[str] = None
    psychological_pressure_explanation: Optional[str] = None
    final_message: Optional[str] = None
    accepted_rewrite: Optional[Union[bool, str]] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class ScenarioResponseSchema(BaseModel):
    """Scenario response schema."""
    id: int
    participant_id: int
    scenario_number: int
    original_input: Optional[str]
    masked_text: Optional[str]
    model: Optional[str]
    suggested_rewrite: Optional[str]
    reasoning: Optional[str]
    risk_level: Optional[str]
    primary_risk_factors: Optional[str]
    linkability_risk_level: Optional[str]
    linkability_risk_explanation: Optional[str]
    authentication_baiting_level: Optional[str]
    authentication_baiting_explanation: Optional[str]
    contextual_alignment_level: Optional[str]
    contextual_alignment_explanation: Optional[str]
    platform_trust_obligation_level: Optional[str]
    platform_trust_obligation_explanation: Optional[str]
    psychological_pressure_level: Optional[str]
    psychological_pressure_explanation: Optional[str]
    final_message: Optional[str]
    accepted_rewrite: Optional[str]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# =============================================================================
# Post-Scenario Survey schemas (Table 4)
# =============================================================================
class PostScenarioSurveyCreate(BaseModel):
    """Post-Scenario Survey creation."""
    scenario_number: int = Field(..., ge=1, le=3)
    confidence_judgment: int = Field(..., ge=1, le=7)
    uncertainty_sharing: int = Field(..., ge=1, le=7)
    perceived_risk: int = Field(..., ge=1, le=7)
    # Group A only (nullable)
    warning_clarity: Optional[Union[int, str]] = None
    warning_helpful: Optional[Union[int, str]] = None
    rewrite_quality: Optional[Union[int, str]] = None


class PostScenarioSurveySchema(BaseModel):
    """Post-Scenario Survey schema."""
    id: int
    participant_id: int
    scenario_number: int
    confidence_judgment: int
    uncertainty_sharing: int
    perceived_risk: int
    warning_clarity: Optional[str]
    warning_helpful: Optional[str]
    rewrite_quality: Optional[str]
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# =============================================================================
# PII Disclosure schemas (Table 5)
# =============================================================================
class PiiDisclosureCreate(BaseModel):
    """PII Disclosure creation - one record per PII type selected."""
    scenario_number: int = Field(..., ge=1, le=3)
    pii_type: str  # Will be validated against enum values
    other_specified: Optional[str] = None


class PiiDisclosureSchema(BaseModel):
    """PII Disclosure schema."""
    id: int
    participant_id: int
    scenario_number: int
    pii_type: str
    other_specified: Optional[str]
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# =============================================================================
# SUS Response schemas (Table 6)
# =============================================================================
class SusResponseCreate(BaseModel):
    """SUS Response creation - Group A only."""
    sus_1: int = Field(..., ge=1, le=5)
    sus_2: int = Field(..., ge=1, le=5)
    sus_3: int = Field(..., ge=1, le=5)
    sus_4: int = Field(..., ge=1, le=5)
    sus_5: int = Field(..., ge=1, le=5)
    sus_6: int = Field(..., ge=1, le=5)
    sus_7: int = Field(..., ge=1, le=5)
    sus_8: int = Field(..., ge=1, le=5)
    sus_9: int = Field(..., ge=1, le=5)
    sus_10: int = Field(..., ge=1, le=5)


class SusResponseSchema(BaseModel):
    """SUS Response schema."""
    id: int
    participant_id: int
    sus_1: int
    sus_2: int
    sus_3: int
    sus_4: int
    sus_5: int
    sus_6: int
    sus_7: int
    sus_8: int
    sus_9: int
    sus_10: int
    sus_score: Optional[float]
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# =============================================================================
# End-of-Study Survey schemas (Table 7)
# =============================================================================
class EndOfStudySurveyCreate(BaseModel):
    """End-of-Study Survey creation."""
    tasks_realistic: int = Field(..., ge=1, le=7)
    realism_explanation: Optional[str] = None
    overall_confidence: int = Field(..., ge=1, le=7)
    sharing_rationale: Optional[str] = None
    # Group A only (nullable)
    trust_system: Optional[Union[int, str]] = None
    trust_explanation: Optional[str] = None


class EndOfStudySurveySchema(BaseModel):
    """End-of-Study Survey schema."""
    id: int
    participant_id: int
    tasks_realistic: int
    realism_explanation: Optional[str]
    overall_confidence: int
    sharing_rationale: Optional[str]
    trust_system: Optional[str]
    trust_explanation: Optional[str]
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# =============================================================================
# Complete Participant Data Response (for retrieving all data)
# =============================================================================
class ParticipantDataResponse(BaseModel):
    """Complete participant data across all tables."""
    participant: ParticipantSchema
    baseline_assessment: Optional[BaselineAssessmentResponse] = None
    scenario_responses: List[ScenarioResponseSchema] = Field(default_factory=list)
    post_scenario_surveys: List[PostScenarioSurveySchema] = Field(default_factory=list)
    pii_disclosures: List[PiiDisclosureSchema] = Field(default_factory=list)
    sus_responses: Optional[SusResponseSchema] = None
    end_of_study_survey: Optional[EndOfStudySurveySchema] = None


class ParticipantProgressResponse(BaseModel):
    """Canonical participant progress state for route and write gating."""
    is_complete: bool
    max_conversation_index_unlocked: int
    survey_unlocked: bool
    completion_unlocked: bool
    redirect_path: str
    allowed_paths: List[str] = Field(default_factory=list)


# =============================================================================
# Legacy schemas for frontend compatibility (deprecated but kept for migration)
# =============================================================================
class ScenarioMessageRecord(BaseModel):
    """Record the final message for a scenario - maps to scenario_responses table."""
    participant_id: str  # prolific_id
    conversation_index: int  # 0, 1, 2 -> maps to scenario_number 1, 2, 3
    final_message: str
    accepted_rewrite: Optional[Union[bool, str]] = None  # true/false/null for A, "[B]" for B
    model: Optional[str] = None  # Actual model used for this scenario's rewrite output
    variant: Optional[str] = None
    # Group A only: PII analysis fields
    original_input: Optional[str] = None  # Text sent to LLM for paired rewrite
    final_masked_text: Optional[str] = None  # Maps to masked_text
    final_rewrite_text: Optional[str] = None  # Maps to suggested_rewrite
    # Full risk analysis payload to persist from Output_1 and Output_2
    risk_level: Optional[str] = None
    primary_risk_factors: Optional[List[str]] = None
    reasoning: Optional[str] = None
    linkability_risk_level: Optional[str] = None
    linkability_risk_explanation: Optional[str] = None
    authentication_baiting_level: Optional[str] = None
    authentication_baiting_explanation: Optional[str] = None
    contextual_alignment_level: Optional[str] = None
    contextual_alignment_explanation: Optional[str] = None
    platform_trust_obligation_level: Optional[str] = None
    platform_trust_obligation_explanation: Optional[str] = None
    psychological_pressure_level: Optional[str] = None
    psychological_pressure_explanation: Optional[str] = None
    # Backward-compatible legacy field
    final_raw_text: Optional[str] = None
