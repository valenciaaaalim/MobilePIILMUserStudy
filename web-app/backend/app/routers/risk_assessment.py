"""
Risk assessment routes.
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
import logging
import sys
import os
import json
import threading
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from pathlib import Path
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.database import SessionLocal, require_db
from app.models import (
    LLMOutput,
    Participant,
    ParticipantLLMUsage,
    ParticipantScenarioLLMUsage,
    ScenarioResponse,
)
from app.services.gemini_service import GeminiService
from app.services.risk_assessment import RiskAssessmentService
from app.config import settings

# Import gliner_service from backend directory
# The file is at web-app/backend/gliner_service.py
# This router is at web-app/backend/app/routers/risk_assessment.py
# So we need to go up two levels: ../../gliner_service.py
backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)
from gliner_service import GliNERService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["risk"])

_gliner_service: Optional[GliNERService] = None
_annotated_conversations: Optional[Dict[int, List[Dict[str, Any]]]] = None


@dataclass
class _SingleFlightState:
    condition: threading.Condition = field(default_factory=threading.Condition)
    active: bool = False
    latest_payload: Optional[Dict[str, Any]] = None
    latest_version: int = 0
    completed_version: int = 0
    completed_result: Optional[Dict[str, Any]] = None
    completed_error: Optional[Exception] = None


class _SingleFlightCoordinator:
    """Coalesce overlapping requests and process only the latest payload per key."""

    def __init__(self):
        self._states: Dict[str, _SingleFlightState] = {}
        self._states_lock = threading.Lock()

    def _get_state(self, key: str) -> _SingleFlightState:
        with self._states_lock:
            state = self._states.get(key)
            if state is None:
                state = _SingleFlightState()
                self._states[key] = state
            return state

    def submit(self, key: str, payload: Dict[str, Any], processor) -> Dict[str, Any]:
        state = self._get_state(key)
        with state.condition:
            state.latest_version += 1
            my_version = state.latest_version
            state.latest_payload = dict(payload)

            if not state.active:
                state.active = True
                threading.Thread(
                    target=self._worker,
                    args=(key, processor),
                    daemon=True,
                ).start()
            else:
                logger.info("[RISK] Coalescing overlapping request (key=%s, version=%d)", key, my_version)
            state.condition.notify_all()

            while state.completed_version < my_version:
                state.condition.wait()

            if state.completed_error is not None:
                raise state.completed_error
            return state.completed_result or {}

    def _worker(self, key: str, processor) -> None:
        state = self._get_state(key)
        while True:
            with state.condition:
                payload = state.latest_payload
                version = state.latest_version
                state.latest_payload = None

            if payload is None:
                with state.condition:
                    if state.latest_payload is not None:
                        continue
                    state.active = False
                    state.condition.notify_all()
                return

            try:
                result = processor(payload)
                error: Optional[Exception] = None
            except Exception as exc:
                result = None
                error = exc

            with state.condition:
                state.completed_result = result
                state.completed_error = error
                state.completed_version = version
                state.condition.notify_all()


_single_flight = _SingleFlightCoordinator()


def _open_db_session() -> Session:
    """Create a DB session bound to the configured engine."""
    return SessionLocal(bind=require_db())


def _resolve_participant_id(db: Session, payload: Dict[str, Any]) -> int:
    """Resolve participant id from request payload."""
    raw_participant_id = payload.get("participant_id")
    if raw_participant_id is not None:
        try:
            participant_id = int(raw_participant_id)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="Invalid participant_id")

        participant = db.query(Participant.id).filter(Participant.id == participant_id).first()
        if participant is None:
            raise HTTPException(status_code=404, detail="Participant not found")
        return participant_id

    prolific_id = payload.get("participant_prolific_id") or payload.get("prolific_id")
    if not prolific_id:
        raise HTTPException(
            status_code=400,
            detail="participant_id or participant_prolific_id is required for LLM usage tracking",
        )

    participant = db.query(Participant.id).filter(Participant.prolific_id == str(prolific_id)).first()
    if participant is None:
        raise HTTPException(status_code=404, detail="Participant not found")
    return int(participant[0])


def _resolve_scenario_id(payload: Dict[str, Any]) -> int:
    """Resolve scenario id from request payload."""
    raw_session_id = payload.get("session_id", 1)
    try:
        scenario_id = int(raw_session_id)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid session_id")
    if scenario_id < 1 or scenario_id > 3:
        raise HTTPException(status_code=400, detail="session_id must be between 1 and 3")
    return scenario_id


def consume_llm_budget(
    db: Session,
    participant_id: int,
    scenario_id: int,
    total_limit: int = 30,
    scenario_limit: int = 10,
) -> Dict[str, Any]:
    """Atomically consume both total and per-scenario LLM budgets."""
    total_stmt = (
        insert(ParticipantLLMUsage)
        .values(participant_id=participant_id, total_calls=1)
        .on_conflict_do_update(
            index_elements=[ParticipantLLMUsage.participant_id],
            set_={"total_calls": ParticipantLLMUsage.total_calls + 1},
            where=ParticipantLLMUsage.total_calls < total_limit,
        )
        .returning(ParticipantLLMUsage.total_calls)
    )

    scenario_stmt = (
        insert(ParticipantScenarioLLMUsage)
        .values(participant_id=participant_id, scenario_id=scenario_id, calls=1)
        .on_conflict_do_update(
            index_elements=[ParticipantScenarioLLMUsage.participant_id, ParticipantScenarioLLMUsage.scenario_id],
            set_={"calls": ParticipantScenarioLLMUsage.calls + 1},
            where=ParticipantScenarioLLMUsage.calls < scenario_limit,
        )
        .returning(ParticipantScenarioLLMUsage.calls)
    )

    try:
        total_calls = db.execute(total_stmt).scalar_one_or_none()
        if total_calls is None:
            db.rollback()
            return {
                "allowed": False,
                "cap_type": "total",
                "limit": total_limit,
                "total_calls": total_limit,
                "scenario_calls": None,
            }

        scenario_calls = db.execute(scenario_stmt).scalar_one_or_none()
        if scenario_calls is None:
            db.rollback()
            return {
                "allowed": False,
                "cap_type": "scenario",
                "limit": scenario_limit,
                "total_calls": None,
                "scenario_calls": scenario_limit,
            }

        db.commit()
        return {
            "allowed": True,
            "cap_type": None,
            "limit": None,
            "total_calls": int(total_calls),
            "scenario_calls": int(scenario_calls),
        }
    except Exception:
        db.rollback()
        raise


def _get_latest_successful_output(
    db: Session,
    participant_id: int,
    scenario_id: int,
) -> Optional[LLMOutput]:
    """Return the latest successful LLM output for participant/scenario."""
    return (
        db.query(LLMOutput)
        .filter(
            LLMOutput.participant_id == participant_id,
            LLMOutput.scenario_id == scenario_id,
            LLMOutput.llm_used.is_(True),
        )
        .order_by(LLMOutput.created_at.desc(), LLMOutput.id.desc())
        .first()
    )


def _is_scenario_submitted(
    db: Session,
    participant_id: int,
    scenario_id: int,
) -> bool:
    """Return whether a scenario already has a submitted final message."""
    row = (
        db.query(ScenarioResponse.id)
        .filter(
            ScenarioResponse.participant_id == participant_id,
            ScenarioResponse.scenario_number == scenario_id,
            ScenarioResponse.final_message.is_not(None),
            ScenarioResponse.final_message != "",
        )
        .first()
    )
    return row is not None


def _persist_llm_output(
    db: Session,
    participant_id: int,
    scenario_id: int,
    llm_used: bool,
    cap_reached: bool,
    response_json: Optional[Dict[str, Any]],
    error_text: Optional[str] = None,
) -> None:
    """Persist LLM output/cap event row."""
    row = LLMOutput(
        participant_id=participant_id,
        scenario_id=scenario_id,
        llm_used=llm_used,
        cap_reached=cap_reached,
        response_json=response_json,
        error_text=error_text,
    )
    db.add(row)
    db.commit()


def get_gliner_service() -> GliNERService:
    """Get or initialize GLiNER service singleton."""
    global _gliner_service
    if _gliner_service is None:
        _gliner_service = GliNERService()
        _gliner_service.initialize()
    return _gliner_service


def transform_messages(raw_messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform messages from {Name, Message} format to frontend-expected format.
    The first name in the conversation is the "contact" (RECEIVED), 
    the second unique name is the "user" (SENT).
    """
    if not raw_messages:
        return []
    
    # Identify the two participants
    names_in_order = []
    for msg in raw_messages:
        name = msg.get("Name", "")
        if name and name not in names_in_order:
            names_in_order.append(name)
            if len(names_in_order) == 2:
                break
    
    # First unique name is contact, second is user
    contact_name = names_in_order[0] if len(names_in_order) > 0 else "Contact"
    user_name = names_in_order[1] if len(names_in_order) > 1 else "User"
    
    transformed = []
    for idx, msg in enumerate(raw_messages):
        name = msg.get("Name", "")
        direction = "RECEIVED" if name == contact_name else "SENT"
        transformed.append({
            "id": f"msg-{idx}",
            "name": name,
            "text": msg.get("Message", ""),
            "direction": direction
        })
    
    return transformed


def load_annotated_conversations(force_reload: bool = False) -> Dict[int, List[Dict[str, Any]]]:
    """Load and cache conversations from annotated_test.json."""
    global _annotated_conversations
    if _annotated_conversations is not None and not force_reload:
        return _annotated_conversations
    
    # Try multiple paths for annotated_test.json
    possible_paths = [
        Path("/app/app/assets/annotated_test.json"),  # Docker mount location
        Path(__file__).resolve().parent.parent / "assets" / "annotated_test.json",  # Local dev
    ]
    
    json_path = None
    for path in possible_paths:
        if path.exists():
            json_path = path
            break
    
    if json_path is None:
        logger.error("annotated_test.json not found in any expected location")
        return {}
    
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        # Map conversation_id (1000, 1001, 1002) to conversation messages
        conversations = data.get("Conversations", [])
        _annotated_conversations = {}
        for idx, conv in enumerate(conversations):
            conv_id = 1000 + idx
            raw_messages = conv.get("Conversation", [])
            # Transform to frontend format
            _annotated_conversations[conv_id] = transform_messages(raw_messages)
        
        logger.info("Loaded %d conversations from %s", len(_annotated_conversations), json_path)
        return _annotated_conversations
    except Exception as e:
        logger.error("Failed to load annotated_test.json: %s", e)
        return {}


def get_conversation_history_from_json(conversation_id: int) -> List[Dict[str, Any]]:
    """Get conversation history from annotated_test.json by conversation_id."""
    conversations = load_annotated_conversations()
    return conversations.get(conversation_id, [])


def _build_risk_service(live_typing: bool) -> RiskAssessmentService:
    """Build risk service with timeout/retry policy based on request type."""
    timeout_seconds = settings.GEMINI_TIMEOUT_SECONDS
    max_attempts = settings.GEMINI_MAX_ATTEMPTS
    if live_typing:
        timeout_seconds = settings.GEMINI_LIVE_TIMEOUT_SECONDS
        max_attempts = settings.GEMINI_LIVE_MAX_ATTEMPTS

    llm = GeminiService(timeout_seconds=timeout_seconds, max_attempts=max_attempts)
    return RiskAssessmentService(llm)


def load_seed_conversations_with_metadata() -> List[Dict[str, Any]]:
    """Load seed conversations with metadata from annotated_test.json."""
    # Force reload to get fresh data
    global _annotated_conversations
    _annotated_conversations = None
    
    # Try multiple paths for annotated_test.json
    possible_paths = [
        Path("/app/app/assets/annotated_test.json"),  # Docker mount location
        Path(__file__).resolve().parent.parent / "assets" / "annotated_test.json",  # Local dev
    ]
    
    json_path = None
    for path in possible_paths:
        if path.exists():
            json_path = path
            break
    
    if json_path is None:
        logger.error("annotated_test.json not found in any expected location")
        return []
    
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        conversations = data.get("Conversations", [])
        result = []
        
        for idx, conv in enumerate(conversations):
            conv_id = 1000 + idx
            raw_messages = conv.get("Conversation", [])
            ground_truth = conv.get("GroundTruth", {})
            
            # Get scenario from ground truth
            scenario = ground_truth.get("Scenario", f"Scenario {idx + 1}")
            
            result.append({
                "conversation_id": conv_id,
                "scenario": scenario,
                "conversation": transform_messages(raw_messages),
                "ground_truth": ground_truth
            })
        
        # Also update the cache
        _annotated_conversations = {
            r["conversation_id"]: r["conversation"] for r in result
        }
        
        logger.info("Loaded %d seed conversations", len(result))
        return result
    except Exception as e:
        logger.error("Failed to load seed conversations: %s", e)
        return []


@router.get("/conversations/seed")
def get_seed_conversations():
    """Get all seed conversations for the study."""
    return load_seed_conversations_with_metadata()


@router.post("/conversations/reload")
def reload_conversations():
    """Force reload conversations from annotated_test.json (for development)."""
    global _annotated_conversations
    _annotated_conversations = None
    result = load_seed_conversations_with_metadata()
    return {"status": "reloaded", "count": len(result)}


def _single_flight_key(payload: Dict[str, Any]) -> str:
    participant = str(
        payload.get("participant_id")
        or payload.get("participant_prolific_id")
        or payload.get("prolific_id")
        or "anonymous"
    )
    session_id = payload.get("session_id", 1)
    return f"{participant}:{session_id}"


def _process_risk_assessment_payload(request: Dict[str, Any]) -> Any:
    """Assess risk payload. Used by single-flight worker."""
    draft_text = request.get("draft_text", "")
    masked_text_input = request.get("masked_text")
    masked_history_input = request.get("masked_history")
    session_id = request.get("session_id", 1)  # Scenario number (1, 2, or 3)
    participant_prolific_id = request.get("participant_prolific_id")
    live_typing = bool(request.get("live_typing"))
    risk_service = _build_risk_service(live_typing=live_typing)
    scenario_id = _resolve_scenario_id(request)
    
    logger.info(
        "[RISK] Processing assessment (session_id=%s, draft_len=%d, live_typing=%s)",
        session_id,
        len(draft_text),
        live_typing,
    )
    
    # Map session_id to conversation_id (1000, 1001, 1002)
    conversation_id = 999 + session_id if session_id <= 3 else 1000
    
    # Use pre-masked text from frontend if provided, otherwise detect PII here
    masked_text = None
    pii_detected = False
    
    if masked_text_input:
        # Frontend already detected PII and provided masked text
        masked_text = masked_text_input
        pii_detected = True
        logger.info("[RISK] Using pre-masked text from frontend (len=%d)", len(masked_text))
    else:
        # Perform PII detection on backend
        try:
            gliner = get_gliner_service()
            pii_result = gliner.mask_and_chunk(draft_text)
            pii_detected = bool(pii_result.pii_spans)
            masked_text = pii_result.masked_text if pii_detected else None
            logger.info("[RISK] Backend PII detection: detected=%s, spans=%d, masked_len=%s", 
                        pii_detected, len(pii_result.pii_spans), len(masked_text) if masked_text else 0)
        except Exception as e:
            logger.error("[RISK] PII masking failed: %s", e, exc_info=True)
            pii_detected = False
            masked_text = None

    if not pii_detected or not masked_text:
        logger.info("[RISK] No PII detected, returning LOW risk without LLM call")
        return {
            "risk_level": "LOW",
            "safer_rewrite": draft_text,
            "show_warning": False,
            "primary_risk_factors": [],
            "reasoning": "",
            "output_1": {
                "linkability_risk": {"level": "", "explanation": ""},
                "authentication_baiting": {"level": "", "explanation": ""},
                "contextual_alignment": {"level": "", "explanation": ""},
                "platform_trust_obligation": {"level": "", "explanation": ""},
                "psychological_pressure": {"level": "", "explanation": ""},
            },
            "output_2": {
                "original_user_message": draft_text,
                "risk_level": "LOW",
                "primary_risk_factors": [],
                "reasoning": "",
                "rewrite": draft_text
            }
        }

    participant_id: Optional[int] = None
    budget_result: Dict[str, Any]
    db = _open_db_session()
    try:
        participant_id = _resolve_participant_id(db, request)
        if _is_scenario_submitted(db, participant_id=participant_id, scenario_id=scenario_id):
            budget_result = {
                "allowed": False,
                "cap_type": "scenario",
                "limit": 10,
                "total_calls": None,
                "scenario_calls": None,
            }
            logger.info(
                "[LLM_CAP] participant_id=%s scenario_id=%s scenario already submitted; reusing final LLM output",
                participant_id,
                scenario_id,
            )
        else:
            budget_result = consume_llm_budget(
                db,
                participant_id=participant_id,
                scenario_id=scenario_id,
                total_limit=30,
                scenario_limit=10,
            )
        if not budget_result["allowed"]:
            cap_type = budget_result["cap_type"]
            limit = int(budget_result["limit"])
            latest_output = _get_latest_successful_output(
                db,
                participant_id=participant_id,
                scenario_id=scenario_id,
            )

            if latest_output and isinstance(latest_output.response_json, dict):
                capped_response = dict(latest_output.response_json)
                capped_response["cap_reached"] = True
                _persist_llm_output(
                    db,
                    participant_id=participant_id,
                    scenario_id=scenario_id,
                    llm_used=False,
                    cap_reached=True,
                    response_json=capped_response,
                    error_text="cap reached",
                )
                logger.info(
                    "[LLM_CAP] participant_id=%s scenario_id=%s llm_used=%s cap_reached=%s cap_type=%s limit=%s",
                    participant_id,
                    scenario_id,
                    False,
                    True,
                    cap_type,
                    limit,
                )
                return capped_response

            _persist_llm_output(
                db,
                participant_id=participant_id,
                scenario_id=scenario_id,
                llm_used=False,
                cap_reached=True,
                response_json=None,
                error_text="cap reached, no previous",
            )
            logger.info(
                "[LLM_CAP] participant_id=%s scenario_id=%s llm_used=%s cap_reached=%s cap_type=%s limit=%s",
                participant_id,
                scenario_id,
                False,
                True,
                cap_type,
                limit,
            )
            return JSONResponse(
                status_code=429,
                content={
                    "detail": "LLM cap reached",
                    "cap_type": cap_type,
                    "limit": limit,
                },
            )

        logger.info(
            "[LLM_CAP] participant_id=%s scenario_id=%s llm_used=%s cap_reached=%s total_calls=%s scenario_calls=%s",
            participant_id,
            scenario_id,
            True,
            False,
            budget_result["total_calls"],
            budget_result["scenario_calls"],
        )
    finally:
        db.close()

    # Get conversation history from annotated_test.json using conversation_id.
    # Frontend can also provide masked_history to avoid remasking history repeatedly.
    conversation_history = get_conversation_history_from_json(conversation_id)
    masked_history = masked_history_input if masked_history_input else None
    logger.info(
        "[RISK] Using conversation history (conv_id=%s, messages=%d, has_masked_history=%s)",
        conversation_id,
        len(conversation_history),
        bool(masked_history)
    )

    logger.info("[RISK] Calling LLM for risk assessment with masked_text (len=%d)...", len(masked_text))
    result = risk_service.assess_risk(
        draft_text=draft_text,
        conversation_history=conversation_history,
        masked_draft=masked_text,  # Pass the masked version to LLM
        masked_history=masked_history,
        session_id=session_id,
        prolific_id=participant_prolific_id
    )
    
    logger.info("[RISK] LLM result: risk_level=%s, has_rewrite=%s, rewrite_len=%d", 
                result["risk_level"], bool(result["safer_rewrite"]), len(result["safer_rewrite"]) if result["safer_rewrite"] else 0)

    response_payload = {
        "risk_level": result["risk_level"],
        "safer_rewrite": result["safer_rewrite"],
        "show_warning": result["show_warning"],
        "primary_risk_factors": result.get("primary_risk_factors", []),
        "reasoning": result.get("reasoning", ""),
        "output_1": result.get("output_1", {}),
        "output_2": result.get("output_2", {})
    }

    if participant_id is not None:
        log_db = _open_db_session()
        try:
            _persist_llm_output(
                log_db,
                participant_id=participant_id,
                scenario_id=scenario_id,
                llm_used=True,
                cap_reached=False,
                response_json=response_payload,
            )
            logger.info(
                "[LLM_OUTPUT] participant_id=%s scenario_id=%s llm_used=%s cap_reached=%s",
                participant_id,
                scenario_id,
                True,
                False,
            )
        finally:
            log_db.close()

    return response_payload


@router.post("/risk/assess")
def assess_risk(request: dict):
    """Assess risk of a draft message with per-session single-flight coalescing."""
    key = _single_flight_key(request)
    logger.info(
        "[RISK] assess_risk endpoint called (key=%s, draft_len=%d)",
        key,
        len(request.get("draft_text", "")),
    )
    return _single_flight.submit(key, request, _process_risk_assessment_payload)
