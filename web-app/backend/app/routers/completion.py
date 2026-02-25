"""
Completion URL routes.
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import Participant
from app.routers.participants import build_completion_url

router = APIRouter(prefix="/api/completion", tags=["completion"])


@router.get("/prolific")
def get_prolific_completion_url(
    participant_id: int | None = None,
    prolific_id: str | None = None,
    db: Session = Depends(get_db)
):
    """Return the Prolific completion URL for a participant."""
    if not prolific_id and participant_id is not None:
        participant = db.query(Participant).filter(Participant.id == participant_id).first()
        if not participant:
            raise HTTPException(status_code=404, detail="Participant not found")
        prolific_id = participant.prolific_id
    return {"completion_url": build_completion_url(prolific_id)}
