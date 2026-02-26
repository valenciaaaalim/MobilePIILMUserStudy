"""
Consent logging routes.
"""
from datetime import datetime, timezone
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import ConsentDecision, Participant
from app.schemas import ConsentDecisionCreate, ConsentDecisionResponse
from app.utils import get_singapore_time

router = APIRouter(prefix="/api/consent", tags=["consent"])


@router.post("", response_model=ConsentDecisionResponse)
def log_consent(
    payload: ConsentDecisionCreate,
    db: Session = Depends(get_db)
):
    """Log consent decision (yes/no) with UTC timestamp."""
    decision = ConsentDecision(
        participant_platform_id=payload.participant_platform_id,
        consent=payload.consent,
        timestamp_utc=datetime.now(timezone.utc)
    )
    db.add(decision)

    # Align participant "start time" with explicit consent-continue action.
    if payload.consent == "yes" and payload.participant_platform_id:
        participant = db.query(Participant).filter(
            Participant.prolific_id == payload.participant_platform_id
        ).first()
        if participant is not None and not participant.is_complete:
            participant.created_at = get_singapore_time().replace(microsecond=0)

    db.commit()
    return ConsentDecisionResponse(status="logged")
