"""
Consent logging routes.
"""
from datetime import datetime, timezone
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import ConsentDecision
from app.schemas import ConsentDecisionCreate, ConsentDecisionResponse

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
    db.commit()
    return ConsentDecisionResponse(status="logged")
