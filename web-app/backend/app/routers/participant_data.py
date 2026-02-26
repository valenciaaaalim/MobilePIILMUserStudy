"""
Normalized participant data routes.
All endpoints write to the 7 normalized tables.
"""
import logging
import json
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import and_
from app.database import get_db
from app.models import (
    Participant,
    BaselineAssessment,
    ScenarioResponse,
    PostScenarioSurvey,
    PiiDisclosure,
    SusResponse,
    EndOfStudySurvey,
    PiiTypeEnum
)
from app.schemas import (
    BaselineAssessmentCreate,
    BaselineAssessmentResponse,
    ScenarioResponseCreate,
    ScenarioResponseSchema,
    ScenarioMessageRecord,
    PostScenarioSurveyCreate,
    PostScenarioSurveySchema,
    PiiDisclosureCreate,
    PiiDisclosureSchema,
    SusResponseCreate,
    SusResponseSchema,
    EndOfStudySurveyCreate,
    EndOfStudySurveySchema,
    ParticipantDataResponse,
    ParticipantSchema
)
from app.utils import ensure_singapore_tz, get_singapore_time

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/participants", tags=["participant-data"])


def calculate_sus_score(sus_1, sus_2, sus_3, sus_4, sus_5, sus_6, sus_7, sus_8, sus_9, sus_10):
    """
    Calculate SUS score from 10 SUS items.
    Odd items (1,3,5,7,9): score = value - 1
    Even items (2,4,6,8,10): score = 5 - value
    Final score = sum of all scores * 2.5
    """
    scores = [
        sus_1 - 1,      # sus_1 (odd)
        5 - sus_2,      # sus_2 (even)
        sus_3 - 1,      # sus_3 (odd)
        5 - sus_4,      # sus_4 (even)
        sus_5 - 1,      # sus_5 (odd)
        5 - sus_6,      # sus_6 (even)
        sus_7 - 1,      # sus_7 (odd)
        5 - sus_8,      # sus_8 (even)
        sus_9 - 1,      # sus_9 (odd)
        5 - sus_10      # sus_10 (even)
    ]
    total = sum(scores)
    sus_score = total * 2.5
    return round(sus_score, 2)


def get_participant_by_id(db: Session, participant_id: int) -> Participant:
    """Get participant by ID or raise 404."""
    participant = db.query(Participant).filter(Participant.id == participant_id).first()
    if not participant:
        raise HTTPException(status_code=404, detail="Participant not found")
    return participant


def get_participant_by_prolific_id(db: Session, prolific_id: str) -> Participant:
    """Get participant by Prolific ID or raise 404."""
    participant = db.query(Participant).filter(Participant.prolific_id == prolific_id).first()
    if not participant:
        raise HTTPException(status_code=404, detail="Participant not found")
    return participant


def build_participant_data_response(db: Session, participant: Participant) -> ParticipantDataResponse:
    """Build full normalized participant response payload."""
    participant_id = participant.id
    baseline_assessment = db.query(BaselineAssessment).filter(
        BaselineAssessment.participant_id == participant_id
    ).first()
    
    scenario_responses = db.query(ScenarioResponse).filter(
        ScenarioResponse.participant_id == participant_id
    ).order_by(ScenarioResponse.scenario_number).all()
    
    post_scenario_surveys = db.query(PostScenarioSurvey).filter(
        PostScenarioSurvey.participant_id == participant_id
    ).order_by(PostScenarioSurvey.scenario_number).all()
    
    pii_disclosures = db.query(PiiDisclosure).filter(
        PiiDisclosure.participant_id == participant_id
    ).order_by(PiiDisclosure.scenario_number, PiiDisclosure.created_at).all()
    
    sus_responses = db.query(SusResponse).filter(
        SusResponse.participant_id == participant_id
    ).first()
    
    end_of_study_survey = db.query(EndOfStudySurvey).filter(
        EndOfStudySurvey.participant_id == participant_id
    ).first()
    
    return ParticipantDataResponse(
        participant=ParticipantSchema.model_validate(participant),
        baseline_assessment=BaselineAssessmentResponse.model_validate(baseline_assessment) if baseline_assessment else None,
        scenario_responses=[ScenarioResponseSchema.model_validate(sr) for sr in scenario_responses],
        post_scenario_surveys=[PostScenarioSurveySchema.model_validate(ps) for ps in post_scenario_surveys],
        pii_disclosures=[PiiDisclosureSchema.model_validate(pd) for pd in pii_disclosures],
        sus_responses=SusResponseSchema.model_validate(sus_responses) if sus_responses else None,
        end_of_study_survey=EndOfStudySurveySchema.model_validate(end_of_study_survey) if end_of_study_survey else None
    )


# =============================================================================
# Baseline Assessment endpoints (Table 2)
# =============================================================================
@router.post("/{participant_id}/baseline-assessment", response_model=BaselineAssessmentResponse)
def create_baseline_assessment(
    participant_id: int,
    data: BaselineAssessmentCreate,
    db: Session = Depends(get_db)
):
    """Create baseline self-assessment for a participant."""
    participant = get_participant_by_id(db, participant_id)
    
    # Check if baseline assessment already exists
    existing = db.query(BaselineAssessment).filter(
        BaselineAssessment.participant_id == participant_id
    ).first()
    if existing:
        raise HTTPException(status_code=409, detail="Baseline assessment already exists")
    
    baseline = BaselineAssessment(
        participant_id=participant_id,
        recognize_sensitive=data.recognize_sensitive,
        avoid_accidental=data.avoid_accidental,
        familiar_scams=data.familiar_scams,
        contextual_judgment=data.contextual_judgment
    )
    db.add(baseline)
    db.commit()
    db.refresh(baseline)
    
    return baseline


# =============================================================================
# Scenario Response endpoints (Table 3)
# =============================================================================
@router.post("/{participant_id}/scenario-response", response_model=ScenarioResponseSchema)
def create_or_update_scenario_response(
    participant_id: int,
    data: ScenarioResponseCreate,
    db: Session = Depends(get_db)
):
    """Create or update scenario response for a participant."""
    participant = get_participant_by_id(db, participant_id)
    
    # Check if scenario response already exists
    existing = db.query(ScenarioResponse).filter(
        and_(
            ScenarioResponse.participant_id == participant_id,
            ScenarioResponse.scenario_number == data.scenario_number
        )
    ).first()
    
    if existing:
        # Update existing record
        if data.original_input is not None:
            existing.original_input = data.original_input
        if data.masked_text is not None:
            existing.masked_text = data.masked_text
        if data.suggested_rewrite is not None:
            existing.suggested_rewrite = data.suggested_rewrite
        if data.reasoning is not None:
            existing.reasoning = data.reasoning
        if data.risk_level is not None:
            existing.risk_level = data.risk_level
        if data.primary_risk_factors is not None:
            existing.primary_risk_factors = data.primary_risk_factors
        if data.linkability_risk_level is not None:
            existing.linkability_risk_level = data.linkability_risk_level
        if data.linkability_risk_explanation is not None:
            existing.linkability_risk_explanation = data.linkability_risk_explanation
        if data.authentication_baiting_level is not None:
            existing.authentication_baiting_level = data.authentication_baiting_level
        if data.authentication_baiting_explanation is not None:
            existing.authentication_baiting_explanation = data.authentication_baiting_explanation
        if data.contextual_alignment_level is not None:
            existing.contextual_alignment_level = data.contextual_alignment_level
        if data.contextual_alignment_explanation is not None:
            existing.contextual_alignment_explanation = data.contextual_alignment_explanation
        if data.platform_trust_obligation_level is not None:
            existing.platform_trust_obligation_level = data.platform_trust_obligation_level
        if data.platform_trust_obligation_explanation is not None:
            existing.platform_trust_obligation_explanation = data.platform_trust_obligation_explanation
        if data.psychological_pressure_level is not None:
            existing.psychological_pressure_level = data.psychological_pressure_level
        if data.psychological_pressure_explanation is not None:
            existing.psychological_pressure_explanation = data.psychological_pressure_explanation
        if data.final_message is not None:
            existing.final_message = data.final_message
        if data.accepted_rewrite is not None:
            existing.accepted_rewrite = data.accepted_rewrite
        db.commit()
        db.refresh(existing)
        return existing
    else:
        # Create new record
        scenario_response = ScenarioResponse(
            participant_id=participant_id,
            scenario_number=data.scenario_number,
            original_input=data.original_input,
            masked_text=data.masked_text,
            suggested_rewrite=data.suggested_rewrite,
            reasoning=data.reasoning,
            risk_level=data.risk_level,
            primary_risk_factors=data.primary_risk_factors,
            linkability_risk_level=data.linkability_risk_level,
            linkability_risk_explanation=data.linkability_risk_explanation,
            authentication_baiting_level=data.authentication_baiting_level,
            authentication_baiting_explanation=data.authentication_baiting_explanation,
            contextual_alignment_level=data.contextual_alignment_level,
            contextual_alignment_explanation=data.contextual_alignment_explanation,
            platform_trust_obligation_level=data.platform_trust_obligation_level,
            platform_trust_obligation_explanation=data.platform_trust_obligation_explanation,
            psychological_pressure_level=data.psychological_pressure_level,
            psychological_pressure_explanation=data.psychological_pressure_explanation,
            final_message=data.final_message,
            accepted_rewrite=data.accepted_rewrite
        )
        db.add(scenario_response)
        db.commit()
        db.refresh(scenario_response)
        return scenario_response


@router.post("/message")
def record_scenario_message(
    data: ScenarioMessageRecord,
    db: Session = Depends(get_db)
):
    """
    Record scenario message data - compatibility endpoint for frontend.
    Maps conversation_index (0,1,2) to scenario_number (1,2,3).
    """
    if data.conversation_index not in (0, 1, 2):
        raise HTTPException(status_code=400, detail="Invalid conversation index")
    
    # Get participant by prolific_id
    participant = get_participant_by_prolific_id(db, data.participant_id)
    
    scenario_number = data.conversation_index + 1  # Convert 0-indexed to 1-indexed
    
    # Check if scenario response already exists
    existing = db.query(ScenarioResponse).filter(
        and_(
            ScenarioResponse.participant_id == participant.id,
            ScenarioResponse.scenario_number == scenario_number
        )
    ).first()
    
    if existing:
        # Update existing record
        existing.final_message = data.final_message
        original_input = data.original_input if data.original_input is not None else data.final_raw_text
        if original_input is not None:
            existing.original_input = original_input
        if data.final_masked_text is not None:
            existing.masked_text = data.final_masked_text
        if data.final_rewrite_text is not None:
            existing.suggested_rewrite = data.final_rewrite_text

        # Persist full Output_1 / Output_2 analysis fields for downstream analysis.
        if data.risk_level is not None:
            existing.risk_level = data.risk_level
        if data.primary_risk_factors is not None:
            existing.primary_risk_factors = json.dumps(data.primary_risk_factors, ensure_ascii=True)
        if data.reasoning is not None:
            existing.reasoning = data.reasoning
        if data.linkability_risk_level is not None:
            existing.linkability_risk_level = data.linkability_risk_level
        if data.linkability_risk_explanation is not None:
            existing.linkability_risk_explanation = data.linkability_risk_explanation
        if data.authentication_baiting_level is not None:
            existing.authentication_baiting_level = data.authentication_baiting_level
        if data.authentication_baiting_explanation is not None:
            existing.authentication_baiting_explanation = data.authentication_baiting_explanation
        if data.contextual_alignment_level is not None:
            existing.contextual_alignment_level = data.contextual_alignment_level
        if data.contextual_alignment_explanation is not None:
            existing.contextual_alignment_explanation = data.contextual_alignment_explanation
        if data.platform_trust_obligation_level is not None:
            existing.platform_trust_obligation_level = data.platform_trust_obligation_level
        if data.platform_trust_obligation_explanation is not None:
            existing.platform_trust_obligation_explanation = data.platform_trust_obligation_explanation
        if data.psychological_pressure_level is not None:
            existing.psychological_pressure_level = data.psychological_pressure_level
        if data.psychological_pressure_explanation is not None:
            existing.psychological_pressure_explanation = data.psychological_pressure_explanation

        # Determine if rewrite was accepted (final message matches rewrite)
        if data.final_rewrite_text and data.final_message:
            existing.accepted_rewrite = data.final_message.strip() == data.final_rewrite_text.strip()
        logger.info(f"[DB] Updated scenario_response for participant {participant.id}, scenario {scenario_number}")
    else:
        # Create new record
        accepted_rewrite = None
        if data.final_rewrite_text and data.final_message:
            accepted_rewrite = data.final_message.strip() == data.final_rewrite_text.strip()
        
        scenario_response = ScenarioResponse(
            participant_id=participant.id,
            scenario_number=scenario_number,
            original_input=data.original_input if data.original_input is not None else data.final_raw_text,
            masked_text=data.final_masked_text,
            suggested_rewrite=data.final_rewrite_text,
            reasoning=data.reasoning,
            risk_level=data.risk_level,
            primary_risk_factors=json.dumps(data.primary_risk_factors, ensure_ascii=True) if data.primary_risk_factors is not None else None,
            linkability_risk_level=data.linkability_risk_level,
            linkability_risk_explanation=data.linkability_risk_explanation,
            authentication_baiting_level=data.authentication_baiting_level,
            authentication_baiting_explanation=data.authentication_baiting_explanation,
            contextual_alignment_level=data.contextual_alignment_level,
            contextual_alignment_explanation=data.contextual_alignment_explanation,
            platform_trust_obligation_level=data.platform_trust_obligation_level,
            platform_trust_obligation_explanation=data.platform_trust_obligation_explanation,
            psychological_pressure_level=data.psychological_pressure_level,
            psychological_pressure_explanation=data.psychological_pressure_explanation,
            final_message=data.final_message,
            accepted_rewrite=accepted_rewrite
        )
        db.add(scenario_response)
        logger.info(f"[DB] Created scenario_response for participant {participant.id}, scenario {scenario_number}")
    
    db.commit()
    return {"status": "saved"}


# =============================================================================
# Post-Scenario Survey endpoints (Table 4)
# =============================================================================
@router.post("/{participant_id}/post-scenario-survey", response_model=PostScenarioSurveySchema)
def create_post_scenario_survey(
    participant_id: int,
    data: PostScenarioSurveyCreate,
    db: Session = Depends(get_db)
):
    """Create post-scenario survey response for a participant."""
    participant = get_participant_by_id(db, participant_id)
    
    # Check if post-scenario survey already exists
    existing = db.query(PostScenarioSurvey).filter(
        and_(
            PostScenarioSurvey.participant_id == participant_id,
            PostScenarioSurvey.scenario_number == data.scenario_number
        )
    ).first()
    if existing:
        raise HTTPException(status_code=409, detail="Post-scenario survey already exists for this scenario")
    
    post_survey = PostScenarioSurvey(
        participant_id=participant_id,
        scenario_number=data.scenario_number,
        confidence_judgment=data.confidence_judgment,
        uncertainty_sharing=data.uncertainty_sharing,
        perceived_risk=data.perceived_risk,
        warning_clarity=data.warning_clarity,
        warning_helpful=data.warning_helpful,
        rewrite_quality=data.rewrite_quality
    )
    db.add(post_survey)
    db.commit()
    db.refresh(post_survey)
    
    return post_survey


# =============================================================================
# PII Disclosure endpoints (Table 5)
# =============================================================================
VALID_PII_TYPES = {e.value for e in PiiTypeEnum}

@router.post("/{participant_id}/pii-disclosure", response_model=PiiDisclosureSchema)
def create_pii_disclosure(
    participant_id: int,
    data: PiiDisclosureCreate,
    db: Session = Depends(get_db)
):
    """Create PII disclosure record for a participant. One record per PII type selected."""
    participant = get_participant_by_id(db, participant_id)
    
    # Validate PII type
    if data.pii_type not in VALID_PII_TYPES:
        raise HTTPException(status_code=400, detail=f"Invalid PII type: {data.pii_type}. Valid types: {VALID_PII_TYPES}")

    # Idempotency: return existing disclosure if already stored
    existing_filters = [
        PiiDisclosure.participant_id == participant_id,
        PiiDisclosure.scenario_number == data.scenario_number,
        PiiDisclosure.pii_type == data.pii_type
    ]
    if data.pii_type == 'other':
        if data.other_specified is None:
            existing_filters.append(PiiDisclosure.other_specified.is_(None))
        else:
            existing_filters.append(PiiDisclosure.other_specified == data.other_specified)
    existing = db.query(PiiDisclosure).filter(and_(*existing_filters)).first()
    if existing:
        return existing
    
    pii_disclosure = PiiDisclosure(
        participant_id=participant_id,
        scenario_number=data.scenario_number,
        pii_type=data.pii_type,
        other_specified=data.other_specified if data.pii_type == 'other' else None
    )
    db.add(pii_disclosure)
    db.commit()
    db.refresh(pii_disclosure)
    
    return pii_disclosure


# =============================================================================
# SUS Response endpoints (Table 6)
# =============================================================================
@router.post("/{participant_id}/sus-responses", response_model=SusResponseSchema)
def create_sus_responses(
    participant_id: int,
    data: SusResponseCreate,
    db: Session = Depends(get_db)
):
    """Create SUS responses for a participant (Group A only)."""
    participant = get_participant_by_id(db, participant_id)
    
    if participant.variant != "A":
        raise HTTPException(status_code=400, detail="SUS responses are only for Group A participants")
    
    # Check if SUS responses already exist
    existing = db.query(SusResponse).filter(
        SusResponse.participant_id == participant_id
    ).first()
    if existing:
        raise HTTPException(status_code=409, detail="SUS responses already exist")
    
    # Calculate SUS score
    sus_score = calculate_sus_score(
        data.sus_1, data.sus_2, data.sus_3, data.sus_4, data.sus_5,
        data.sus_6, data.sus_7, data.sus_8, data.sus_9, data.sus_10
    )
    
    sus_response = SusResponse(
        participant_id=participant_id,
        sus_1=data.sus_1,
        sus_2=data.sus_2,
        sus_3=data.sus_3,
        sus_4=data.sus_4,
        sus_5=data.sus_5,
        sus_6=data.sus_6,
        sus_7=data.sus_7,
        sus_8=data.sus_8,
        sus_9=data.sus_9,
        sus_10=data.sus_10,
        sus_score=sus_score
    )
    db.add(sus_response)
    db.commit()
    db.refresh(sus_response)
    
    return sus_response


# =============================================================================
# End-of-Study Survey endpoints (Table 7)
# =============================================================================
@router.post("/{participant_id}/end-of-study-survey", response_model=EndOfStudySurveySchema)
def create_end_of_study_survey(
    participant_id: int,
    data: EndOfStudySurveyCreate,
    db: Session = Depends(get_db)
):
    """Create end-of-study survey response for a participant."""
    participant = get_participant_by_id(db, participant_id)
    
    # Check if end-of-study survey already exists
    existing = db.query(EndOfStudySurvey).filter(
        EndOfStudySurvey.participant_id == participant_id
    ).first()
    if existing:
        raise HTTPException(status_code=409, detail="End-of-study survey already exists")
    
    end_survey = EndOfStudySurvey(
        participant_id=participant_id,
        tasks_realistic=data.tasks_realistic,
        realism_explanation=data.realism_explanation,
        overall_confidence=data.overall_confidence,
        sharing_rationale=data.sharing_rationale,
        trust_system=data.trust_system,
        trust_explanation=data.trust_explanation
    )
    db.add(end_survey)
    
    # Mark participant as complete
    participant.is_complete = True
    completion_time = get_singapore_time().replace(microsecond=0)
    if participant.completed_at is None:
        participant.completed_at = completion_time

    # Calculate duration safely for SQLite rows that may deserialize as naive datetimes.
    created_at = ensure_singapore_tz(participant.created_at)
    completed_at = ensure_singapore_tz(participant.completed_at)
    if completed_at is not None:
        participant.completed_at = completed_at
    if created_at is not None and completed_at is not None:
        participant.duration_seconds = max((completed_at - created_at).total_seconds(), 0.0)

    # Mark all scenario rows as completed when participant submits final survey.
    if participant.completed_at is not None:
        db.query(ScenarioResponse).filter(
            ScenarioResponse.participant_id == participant_id,
            ScenarioResponse.completed_at.is_(None)
        ).update(
            {ScenarioResponse.completed_at: participant.completed_at},
            synchronize_session=False
        )
    
    db.commit()
    db.refresh(end_survey)
    
    return end_survey


# =============================================================================
# Get all participant data
# =============================================================================
@router.get("/{participant_id}/data", response_model=ParticipantDataResponse)
def get_participant_data(
    participant_id: int,
    db: Session = Depends(get_db)
):
    """Get all data for a single participant across all tables."""
    participant = get_participant_by_id(db, participant_id)
    return build_participant_data_response(db, participant)


@router.get("/by-prolific/{prolific_id}/data", response_model=ParticipantDataResponse)
def get_participant_data_by_prolific_id(
    prolific_id: str,
    db: Session = Depends(get_db)
):
    """Get all data for a single participant using Prolific ID."""
    participant = get_participant_by_prolific_id(db, prolific_id)
    return build_participant_data_response(db, participant)
