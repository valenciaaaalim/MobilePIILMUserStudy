"""
Participant activity/completion state helpers.
"""
from __future__ import annotations

from datetime import timedelta, datetime
from typing import Optional, Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models import (
    Participant,
    BaselineAssessment,
    ScenarioResponse,
    PostScenarioSurvey,
    SusResponse,
    EndOfStudySurvey,
    LLMOutput,
)
from app.utils import ensure_singapore_tz, get_singapore_time

INACTIVITY_DAYS = 3
COMPLETE_STATE_PROGRESS = "Progress"
COMPLETE_STATE_TRUE = "True"
COMPLETE_STATE_FALSE = "False"


def normalize_completion_state(value: Any) -> str:
    """Normalize legacy bool/null/string values into Progress|True|False."""
    if isinstance(value, bool):
        return COMPLETE_STATE_TRUE if value else COMPLETE_STATE_FALSE
    if value is None:
        return COMPLETE_STATE_PROGRESS
    raw = str(value).strip().lower()
    if raw in {"true", "t", "1", "yes", "y", "[v]"}:
        return COMPLETE_STATE_TRUE
    if raw in {"false", "f", "0", "no", "n"}:
        return COMPLETE_STATE_FALSE
    if raw in {"progress", "in progress", "in_progress", "null", "none", ""}:
        return COMPLETE_STATE_PROGRESS
    return COMPLETE_STATE_PROGRESS


def is_completed_state(value: Any) -> bool:
    """Return True only when value represents a completed participant."""
    return normalize_completion_state(value) == COMPLETE_STATE_TRUE


def _latest_participant_activity_at(db: Session, participant: Participant) -> Optional[datetime]:
    """Return the latest known activity timestamp for a participant."""
    participant_id = participant.id
    candidates = [ensure_singapore_tz(participant.created_at)]

    def add_max(model, column):
        value = db.query(func.max(column)).filter(model.participant_id == participant_id).scalar()
        value = ensure_singapore_tz(value)
        if value is not None:
            candidates.append(value)

    add_max(BaselineAssessment, BaselineAssessment.created_at)
    add_max(ScenarioResponse, ScenarioResponse.created_at)
    add_max(ScenarioResponse, ScenarioResponse.completed_at)
    add_max(PostScenarioSurvey, PostScenarioSurvey.created_at)
    add_max(SusResponse, SusResponse.created_at)
    add_max(EndOfStudySurvey, EndOfStudySurvey.created_at)
    add_max(LLMOutput, LLMOutput.called_at)

    candidates = [c for c in candidates if c is not None]
    if not candidates:
        return None
    return max(candidates)


def sync_participant_completion_state(
    db: Session,
    participant: Participant,
    mark_active: bool = False,
) -> Participant:
    """
    Sync participant.is_complete:
    - "True" when completed_at exists.
    - "False" when inactive for > INACTIVITY_DAYS.
    - "Progress" when incomplete but currently active/within threshold.
    """
    target: str

    if participant.completed_at is not None:
        target = COMPLETE_STATE_TRUE
    elif mark_active:
        target = COMPLETE_STATE_PROGRESS
    else:
        latest_activity = _latest_participant_activity_at(db, participant)
        if latest_activity is None:
            latest_activity = ensure_singapore_tz(participant.created_at)

        if latest_activity is None:
            target = COMPLETE_STATE_PROGRESS
        else:
            inactive_for = get_singapore_time() - latest_activity
            target = COMPLETE_STATE_FALSE if inactive_for > timedelta(days=INACTIVITY_DAYS) else COMPLETE_STATE_PROGRESS

    if normalize_completion_state(participant.is_complete) != target:
        participant.is_complete = target
        db.add(participant)
        db.commit()
        db.refresh(participant)

    return participant


def sync_all_participant_completion_states(db: Session) -> None:
    """Sweep all participants and apply inactivity/completion state rules."""
    participants = db.query(Participant).all()
    for participant in participants:
        sync_participant_completion_state(db, participant, mark_active=False)
