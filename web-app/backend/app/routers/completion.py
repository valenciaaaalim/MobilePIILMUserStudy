"""
Completion URL routes.
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import Participant
from app.participant_state import sync_participant_completion_state
from app.routers.participants import build_completion_url
from app.routers.participant_data import build_participant_progress_response

router = APIRouter(prefix="/api/completion", tags=["completion"])


@router.get("/prolific")
def get_prolific_completion_url(
    participant_id: int | None = None,
    prolific_id: str | None = None,
    db: Session = Depends(get_db)
):
    """Return the Prolific completion URL only when completion is unlocked."""
    participant = None
    if participant_id is not None:
        participant = db.query(Participant).filter(Participant.id == participant_id).first()
    elif prolific_id:
        participant = db.query(Participant).filter(Participant.prolific_id == prolific_id).first()
    else:
        raise HTTPException(
            status_code=400,
            detail="participant_id or prolific_id is required",
        )

    if not participant:
        raise HTTPException(status_code=404, detail="Participant not found")

    participant = sync_participant_completion_state(db, participant, mark_active=False)
    progress = build_participant_progress_response(db, participant)
    if not progress.completion_unlocked:
        raise HTTPException(
            status_code=409,
            detail={"message": "Step out of sequence", "redirect_path": progress.redirect_path},
        )

    return {"completion_url": build_completion_url(participant.prolific_id)}
