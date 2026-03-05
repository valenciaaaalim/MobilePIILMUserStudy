"""
Normalized participant data routes.
All endpoints write to normalized study tables.
"""
import logging
import json
from typing import Any
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import and_
from app.database import get_db
from app.models import (
    Participant,
    BaselineAssessment,
    ScenarioResponse,
    PostScenarioSurvey,
    SusResponse,
    EndOfStudySurvey
)
from app.schemas import (
    BaselineAssessmentCreate,
    BaselineAssessmentResponse,
    ScenarioResponseCreate,
    ScenarioResponseSchema,
    ScenarioMessageRecord,
    PostScenarioSurveyCreate,
    PostScenarioSurveySchema,
    SusResponseCreate,
    SusResponseSchema,
    EndOfStudySurveyCreate,
    EndOfStudySurveySchema,
    ParticipantDataResponse,
    ParticipantSchema,
    ParticipantProgressResponse,
)
from app.utils import ensure_singapore_tz, get_singapore_time
from app.participant_state import (
    sync_participant_completion_state,
    is_completed_state,
    COMPLETE_STATE_TRUE,
)

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
    return sync_participant_completion_state(db, participant, mark_active=False)


def get_participant_by_prolific_id(db: Session, prolific_id: str) -> Participant:
    """Get participant by Prolific ID or raise 404."""
    participant = db.query(Participant).filter(Participant.prolific_id == prolific_id).first()
    if not participant:
        raise HTTPException(status_code=404, detail="Participant not found")
    return sync_participant_completion_state(db, participant, mark_active=False)


def _is_variant_b(variant: str | None) -> bool:
    """Return True when participant is variant B."""
    return (variant or "").strip().upper() == "B"


def _normalize_accepted_rewrite(value: Any, variant: str | None) -> str | None:
    """
    Normalize accepted_rewrite for storage:
    - Variant B => "[B]"
    - Variant A => "true" | "false" | null
    """
    if _is_variant_b(variant):
        return "[B]"
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized == "true":
            return "true"
        if normalized == "false":
            return "false"
        if normalized == "abort":
            return "ABORT"
        if normalized in {"", "null", "none"}:
            return None
    return None


def _variant_a_only_value(value: Any, variant: str | None) -> Any:
    """Set Variant A-only fields to [B] for variant B participants."""
    if _is_variant_b(variant):
        return "[B]"
    return value


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
        pii_disclosures=[],
        sus_responses=SusResponseSchema.model_validate(sus_responses) if sus_responses else None,
        end_of_study_survey=EndOfStudySurveySchema.model_validate(end_of_study_survey) if end_of_study_survey else None
    )


def build_participant_progress_response(db: Session, participant: Participant) -> ParticipantProgressResponse:
    """Compute canonical progression state and redirect target for a participant."""
    participant_id = participant.id
    baseline_exists = db.query(BaselineAssessment.id).filter(
        BaselineAssessment.participant_id == participant_id
    ).first() is not None
    end_of_study_exists = db.query(EndOfStudySurvey.id).filter(
        EndOfStudySurvey.participant_id == participant_id
    ).first() is not None

    scenario_rows = db.query(
        ScenarioResponse.scenario_number,
        ScenarioResponse.final_message
    ).filter(
        ScenarioResponse.participant_id == participant_id
    ).all()
    scenario_final_submitted = {
        int(row[0]) for row in scenario_rows
        if row[1] is not None and str(row[1]).strip() != ""
    }

    post_scenario_numbers = {
        int(row[0]) for row in db.query(PostScenarioSurvey.scenario_number).filter(
            PostScenarioSurvey.participant_id == participant_id
        ).all()
    }

    if is_completed_state(participant.is_complete) or participant.completed_at is not None or end_of_study_exists:
        return ParticipantProgressResponse(
            is_complete=True,
            max_conversation_index_unlocked=2,
            survey_unlocked=False,
            completion_unlocked=True,
            redirect_path="/completion",
            allowed_paths=["/completion"],
        )

    if not baseline_exists:
        return ParticipantProgressResponse(
            is_complete=False,
            max_conversation_index_unlocked=-1,
            survey_unlocked=True,
            completion_unlocked=False,
            redirect_path="/",
            allowed_paths=["/", "/survey/pre", "/survey/baseline"],
        )

    for scenario_number in (1, 2, 3):
        conversation_index = scenario_number - 1
        if scenario_number not in scenario_final_submitted:
            path = f"/conversation/{conversation_index}"
            return ParticipantProgressResponse(
                is_complete=False,
                max_conversation_index_unlocked=conversation_index,
                survey_unlocked=False,
                completion_unlocked=False,
                redirect_path=path,
                allowed_paths=[path],
            )
        if scenario_number not in post_scenario_numbers:
            mid_path = f"/survey/mid?index={conversation_index}"
            post_path = f"/survey/post-scenario?index={conversation_index}"
            return ParticipantProgressResponse(
                is_complete=False,
                max_conversation_index_unlocked=conversation_index,
                survey_unlocked=True,
                completion_unlocked=False,
                redirect_path=mid_path,
                allowed_paths=[mid_path, post_path],
            )

    return ParticipantProgressResponse(
        is_complete=False,
        max_conversation_index_unlocked=2,
        survey_unlocked=True,
        completion_unlocked=False,
        redirect_path="/survey/end-of-study",
        allowed_paths=["/survey/end-of-study", "/survey/post"],
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
    participant = sync_participant_completion_state(db, participant, mark_active=True)
    progress = build_participant_progress_response(db, participant)
    if progress.redirect_path not in {"/", "/survey/pre", "/survey/baseline"}:
        raise HTTPException(
            status_code=409,
            detail={"message": "Step out of sequence", "redirect_path": progress.redirect_path},
        )
    
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
        contextual_judgment=data.contextual_judgment,
        participant_variant=participant.variant,
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
    participant = sync_participant_completion_state(db, participant, mark_active=True)
    
    # Check if scenario response already exists
    existing = db.query(ScenarioResponse).filter(
        and_(
            ScenarioResponse.participant_id == participant_id,
            ScenarioResponse.scenario_number == data.scenario_number
        )
    ).first()
    
    accepted_rewrite_value = _normalize_accepted_rewrite(data.accepted_rewrite, participant.variant)
    variant_b = _is_variant_b(participant.variant)
    original_input_value = _variant_a_only_value(data.original_input, participant.variant)
    masked_text_value = _variant_a_only_value(data.masked_text, participant.variant)
    output_id_value = _variant_a_only_value(data.output_id, participant.variant)
    total_tokens_value = None if variant_b else data.total_tokens
    input_tokens_value = None if variant_b else data.input_tokens
    model_value = _variant_a_only_value(data.model, participant.variant)
    suggested_rewrite_value = _variant_a_only_value(data.suggested_rewrite, participant.variant)
    risk_level_value = "[B]" if variant_b else data.risk_level
    reasoning_value = "[B]" if variant_b else data.reasoning
    primary_risk_factors_value = "[B]" if variant_b else data.primary_risk_factors
    linkability_risk_level_value = "[B]" if variant_b else data.linkability_risk_level
    linkability_risk_explanation_value = "[B]" if variant_b else data.linkability_risk_explanation
    authentication_baiting_level_value = "[B]" if variant_b else data.authentication_baiting_level
    authentication_baiting_explanation_value = "[B]" if variant_b else data.authentication_baiting_explanation
    contextual_alignment_level_value = "[B]" if variant_b else data.contextual_alignment_level
    contextual_alignment_explanation_value = "[B]" if variant_b else data.contextual_alignment_explanation
    platform_trust_obligation_level_value = "[B]" if variant_b else data.platform_trust_obligation_level
    platform_trust_obligation_explanation_value = "[B]" if variant_b else data.platform_trust_obligation_explanation
    psychological_pressure_level_value = "[B]" if variant_b else data.psychological_pressure_level
    psychological_pressure_explanation_value = "[B]" if variant_b else data.psychological_pressure_explanation

    if existing:
        # Update existing record
        if existing.participant_variant is None:
            existing.participant_variant = participant.variant
        if original_input_value is not None:
            existing.original_input = original_input_value
        if masked_text_value is not None:
            existing.masked_text = masked_text_value
        if output_id_value is not None:
            existing.output_id = output_id_value
        if total_tokens_value is not None:
            existing.total_tokens = total_tokens_value
        if input_tokens_value is not None:
            existing.input_tokens = input_tokens_value
        if model_value is not None:
            existing.model = model_value
        if suggested_rewrite_value is not None:
            existing.suggested_rewrite = suggested_rewrite_value
        if reasoning_value is not None:
            existing.reasoning = reasoning_value
        if risk_level_value is not None:
            existing.risk_level = risk_level_value
        if primary_risk_factors_value is not None:
            existing.primary_risk_factors = primary_risk_factors_value
        if linkability_risk_level_value is not None:
            existing.linkability_risk_level = linkability_risk_level_value
        if linkability_risk_explanation_value is not None:
            existing.linkability_risk_explanation = linkability_risk_explanation_value
        if authentication_baiting_level_value is not None:
            existing.authentication_baiting_level = authentication_baiting_level_value
        if authentication_baiting_explanation_value is not None:
            existing.authentication_baiting_explanation = authentication_baiting_explanation_value
        if contextual_alignment_level_value is not None:
            existing.contextual_alignment_level = contextual_alignment_level_value
        if contextual_alignment_explanation_value is not None:
            existing.contextual_alignment_explanation = contextual_alignment_explanation_value
        if platform_trust_obligation_level_value is not None:
            existing.platform_trust_obligation_level = platform_trust_obligation_level_value
        if platform_trust_obligation_explanation_value is not None:
            existing.platform_trust_obligation_explanation = platform_trust_obligation_explanation_value
        if psychological_pressure_level_value is not None:
            existing.psychological_pressure_level = psychological_pressure_level_value
        if psychological_pressure_explanation_value is not None:
            existing.psychological_pressure_explanation = psychological_pressure_explanation_value
        if data.final_message is not None:
            existing.final_message = data.final_message
        if accepted_rewrite_value is not None:
            existing.accepted_rewrite = accepted_rewrite_value
        db.commit()
        db.refresh(existing)
        return existing
    else:
        # Create new record
        scenario_response = ScenarioResponse(
            participant_id=participant_id,
            scenario_number=data.scenario_number,
            original_input=original_input_value,
            masked_text=masked_text_value,
            output_id=output_id_value,
            total_tokens=total_tokens_value,
            input_tokens=input_tokens_value,
            model=model_value,
            suggested_rewrite=suggested_rewrite_value,
            reasoning=reasoning_value,
            risk_level=risk_level_value,
            primary_risk_factors=primary_risk_factors_value,
            linkability_risk_level=linkability_risk_level_value,
            linkability_risk_explanation=linkability_risk_explanation_value,
            authentication_baiting_level=authentication_baiting_level_value,
            authentication_baiting_explanation=authentication_baiting_explanation_value,
            contextual_alignment_level=contextual_alignment_level_value,
            contextual_alignment_explanation=contextual_alignment_explanation_value,
            platform_trust_obligation_level=platform_trust_obligation_level_value,
            platform_trust_obligation_explanation=platform_trust_obligation_explanation_value,
            psychological_pressure_level=psychological_pressure_level_value,
            psychological_pressure_explanation=psychological_pressure_explanation_value,
            final_message=data.final_message,
            accepted_rewrite=accepted_rewrite_value,
            participant_variant=participant.variant,
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
    participant = sync_participant_completion_state(db, participant, mark_active=True)
    
    scenario_number = data.conversation_index + 1  # Convert 0-indexed to 1-indexed
    progress = build_participant_progress_response(db, participant)
    expected_path = f"/conversation/{data.conversation_index}"
    if progress.redirect_path != expected_path:
        raise HTTPException(
            status_code=409,
            detail={"message": "Step out of sequence", "redirect_path": progress.redirect_path},
        )

    if is_completed_state(participant.is_complete):
        raise HTTPException(status_code=409, detail="Participant already completed")

    scenario_survey_exists = db.query(PostScenarioSurvey.id).filter(
        and_(
            PostScenarioSurvey.participant_id == participant.id,
            PostScenarioSurvey.scenario_number == scenario_number
        )
    ).first()
    if scenario_survey_exists:
        raise HTTPException(status_code=409, detail="Scenario already completed")
    
    # Check if scenario response already exists
    existing = db.query(ScenarioResponse).filter(
        and_(
            ScenarioResponse.participant_id == participant.id,
            ScenarioResponse.scenario_number == scenario_number
        )
    ).first()

    if existing and existing.final_message and existing.final_message.strip():
        raise HTTPException(status_code=409, detail="Scenario message already submitted")

    completion_time = get_singapore_time()
    participant_variant = participant.variant
    variant_b = _is_variant_b(participant_variant)
    scenario_model = _variant_a_only_value(data.model, participant_variant)
    accepted_rewrite_value = _normalize_accepted_rewrite(data.accepted_rewrite, participant_variant)
    original_input_value = _variant_a_only_value(data.original_input, participant_variant)
    masked_text_value = _variant_a_only_value(data.final_masked_text, participant_variant)
    rewrite_text_value = _variant_a_only_value(data.final_rewrite_text, participant_variant)
    output_id_value = _variant_a_only_value(data.output_id, participant_variant)
    total_tokens_value = None if variant_b else data.total_tokens
    input_tokens_value = None if variant_b else data.input_tokens
    risk_level_value = "[B]" if variant_b else data.risk_level
    reasoning_value = "[B]" if variant_b else data.reasoning
    primary_risk_factors_value = "[B]" if variant_b else (
        json.dumps(data.primary_risk_factors, ensure_ascii=True) if data.primary_risk_factors is not None else None
    )
    linkability_risk_level_value = "[B]" if variant_b else data.linkability_risk_level
    linkability_risk_explanation_value = "[B]" if variant_b else data.linkability_risk_explanation
    authentication_baiting_level_value = "[B]" if variant_b else data.authentication_baiting_level
    authentication_baiting_explanation_value = "[B]" if variant_b else data.authentication_baiting_explanation
    contextual_alignment_level_value = "[B]" if variant_b else data.contextual_alignment_level
    contextual_alignment_explanation_value = "[B]" if variant_b else data.contextual_alignment_explanation
    platform_trust_obligation_level_value = "[B]" if variant_b else data.platform_trust_obligation_level
    platform_trust_obligation_explanation_value = "[B]" if variant_b else data.platform_trust_obligation_explanation
    psychological_pressure_level_value = "[B]" if variant_b else data.psychological_pressure_level
    psychological_pressure_explanation_value = "[B]" if variant_b else data.psychological_pressure_explanation

    if existing:
        # Update existing record
        if existing.participant_variant is None:
            existing.participant_variant = participant_variant
        existing.final_message = data.final_message
        existing.completed_at = completion_time
        if original_input_value is not None:
            existing.original_input = original_input_value
        if masked_text_value is not None:
            existing.masked_text = masked_text_value
        if rewrite_text_value is not None:
            existing.suggested_rewrite = rewrite_text_value
        if output_id_value is not None:
            existing.output_id = output_id_value
        if total_tokens_value is not None:
            existing.total_tokens = total_tokens_value
        if input_tokens_value is not None:
            existing.input_tokens = input_tokens_value
        if scenario_model is not None:
            existing.model = scenario_model

        # Persist full Output_1 / Output_2 analysis fields for downstream analysis.
        if risk_level_value is not None:
            existing.risk_level = risk_level_value
        if primary_risk_factors_value is not None:
            existing.primary_risk_factors = primary_risk_factors_value
        if reasoning_value is not None:
            existing.reasoning = reasoning_value
        if linkability_risk_level_value is not None:
            existing.linkability_risk_level = linkability_risk_level_value
        if linkability_risk_explanation_value is not None:
            existing.linkability_risk_explanation = linkability_risk_explanation_value
        if authentication_baiting_level_value is not None:
            existing.authentication_baiting_level = authentication_baiting_level_value
        if authentication_baiting_explanation_value is not None:
            existing.authentication_baiting_explanation = authentication_baiting_explanation_value
        if contextual_alignment_level_value is not None:
            existing.contextual_alignment_level = contextual_alignment_level_value
        if contextual_alignment_explanation_value is not None:
            existing.contextual_alignment_explanation = contextual_alignment_explanation_value
        if platform_trust_obligation_level_value is not None:
            existing.platform_trust_obligation_level = platform_trust_obligation_level_value
        if platform_trust_obligation_explanation_value is not None:
            existing.platform_trust_obligation_explanation = platform_trust_obligation_explanation_value
        if psychological_pressure_level_value is not None:
            existing.psychological_pressure_level = psychological_pressure_level_value
        if psychological_pressure_explanation_value is not None:
            existing.psychological_pressure_explanation = psychological_pressure_explanation_value

        # Persist explicit UI decision:
        # true -> user clicked "Accept safer rewrite"
        # false -> user clicked "Continue anyway"
        # null -> neither button was clicked before submit
        existing.accepted_rewrite = accepted_rewrite_value
        logger.info(f"[DB] Updated scenario_response for participant {participant.id}, scenario {scenario_number}")
    else:
        # Create new record
        scenario_response = ScenarioResponse(
            participant_id=participant.id,
            scenario_number=scenario_number,
            original_input=original_input_value,
            masked_text=masked_text_value,
            output_id=output_id_value,
            total_tokens=total_tokens_value,
            input_tokens=input_tokens_value,
            model=scenario_model,
            suggested_rewrite=rewrite_text_value,
            reasoning=reasoning_value,
            risk_level=risk_level_value,
            primary_risk_factors=primary_risk_factors_value,
            linkability_risk_level=linkability_risk_level_value,
            linkability_risk_explanation=linkability_risk_explanation_value,
            authentication_baiting_level=authentication_baiting_level_value,
            authentication_baiting_explanation=authentication_baiting_explanation_value,
            contextual_alignment_level=contextual_alignment_level_value,
            contextual_alignment_explanation=contextual_alignment_explanation_value,
            platform_trust_obligation_level=platform_trust_obligation_level_value,
            platform_trust_obligation_explanation=platform_trust_obligation_explanation_value,
            psychological_pressure_level=psychological_pressure_level_value,
            psychological_pressure_explanation=psychological_pressure_explanation_value,
            final_message=data.final_message,
            accepted_rewrite=accepted_rewrite_value,
            completed_at=completion_time,
            participant_variant=participant_variant,
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
    participant = sync_participant_completion_state(db, participant, mark_active=True)
    progress = build_participant_progress_response(db, participant)
    expected_mid = f"/survey/mid?index={data.scenario_number - 1}"
    expected_post = f"/survey/post-scenario?index={data.scenario_number - 1}"
    if progress.redirect_path not in {expected_mid, expected_post}:
        raise HTTPException(
            status_code=409,
            detail={"message": "Step out of sequence", "redirect_path": progress.redirect_path},
        )
    
    # Check if post-scenario survey already exists
    existing = db.query(PostScenarioSurvey).filter(
        and_(
            PostScenarioSurvey.participant_id == participant_id,
            PostScenarioSurvey.scenario_number == data.scenario_number
        )
    ).first()
    if existing:
        raise HTTPException(status_code=409, detail="Post-scenario survey already exists for this scenario")

    included_pii_types_value = json.dumps(data.included_pii_types or [], ensure_ascii=True)
    included_pii_other_text_value = (data.included_pii_other_text or "").strip() or None
    variant_b = _is_variant_b(participant.variant)
    warning_clarity_value = "[B]" if variant_b else (str(data.warning_clarity) if data.warning_clarity is not None else None)
    warning_helpful_value = "[B]" if variant_b else (str(data.warning_helpful) if data.warning_helpful is not None else None)
    rewrite_quality_value = "[B]" if variant_b else (str(data.rewrite_quality) if data.rewrite_quality is not None else None)

    post_survey = PostScenarioSurvey(
        participant_id=participant_id,
        scenario_number=data.scenario_number,
        confidence_judgment=data.confidence_judgment,
        uncertainty_sharing=data.uncertainty_sharing,
        perceived_risk=data.perceived_risk,
        included_pii_types=included_pii_types_value,
        included_pii_other_text=included_pii_other_text_value,
        warning_clarity=warning_clarity_value,
        warning_helpful=warning_helpful_value,
        rewrite_quality=rewrite_quality_value,
        participant_variant=participant.variant,
    )
    db.add(post_survey)
    db.commit()
    db.refresh(post_survey)
    
    return post_survey


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
    participant = sync_participant_completion_state(db, participant, mark_active=True)
    progress = build_participant_progress_response(db, participant)
    if progress.redirect_path not in {"/survey/end-of-study", "/survey/post"}:
        raise HTTPException(
            status_code=409,
            detail={"message": "Step out of sequence", "redirect_path": progress.redirect_path},
        )
    
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
        sus_score=sus_score,
        participant_variant=participant.variant,
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
    participant = sync_participant_completion_state(db, participant, mark_active=True)
    progress = build_participant_progress_response(db, participant)
    if progress.redirect_path not in {"/survey/end-of-study", "/survey/post"}:
        raise HTTPException(
            status_code=409,
            detail={"message": "Step out of sequence", "redirect_path": progress.redirect_path},
        )
    
    # Check if end-of-study survey already exists
    existing = db.query(EndOfStudySurvey).filter(
        EndOfStudySurvey.participant_id == participant_id
    ).first()
    if existing:
        raise HTTPException(status_code=409, detail="End-of-study survey already exists")
    variant_b = _is_variant_b(participant.variant)
    trust_system_value = "[B]" if variant_b else (str(data.trust_system) if data.trust_system is not None else None)
    trust_explanation_value = "[B]" if variant_b else data.trust_explanation

    end_survey = EndOfStudySurvey(
        participant_id=participant_id,
        tasks_realistic=data.tasks_realistic,
        realism_explanation=data.realism_explanation,
        overall_confidence=data.overall_confidence,
        sharing_rationale=data.sharing_rationale,
        trust_system=trust_system_value,
        trust_explanation=trust_explanation_value,
        participant_variant=participant.variant,
    )
    db.add(end_survey)
    
    # Mark participant as complete
    participant.is_complete = COMPLETE_STATE_TRUE
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


@router.get("/{participant_id}/progress", response_model=ParticipantProgressResponse)
def get_participant_progress(
    participant_id: int,
    db: Session = Depends(get_db)
):
    """Get canonical participant progress and next allowed route."""
    participant = get_participant_by_id(db, participant_id)
    return build_participant_progress_response(db, participant)


@router.get("/by-prolific/{prolific_id}/data", response_model=ParticipantDataResponse)
def get_participant_data_by_prolific_id(
    prolific_id: str,
    db: Session = Depends(get_db)
):
    """Get all data for a single participant using Prolific ID."""
    participant = get_participant_by_prolific_id(db, prolific_id)
    return build_participant_data_response(db, participant)
