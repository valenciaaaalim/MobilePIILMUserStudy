"""
Risk assessment routes.
"""
from fastapi import APIRouter, Depends
import logging
import sys
import os
import json
from typing import Optional, List, Dict, Any
from pathlib import Path
from app.services.gemini_service import GeminiService
from app.services.risk_assessment import RiskAssessmentService

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


def get_risk_assessment_service() -> RiskAssessmentService:
    """Dependency to get risk assessment service."""
    return RiskAssessmentService(GeminiService())


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


@router.post("/risk/assess")
def assess_risk(
    request: dict,
    risk_service: RiskAssessmentService = Depends(get_risk_assessment_service)
):
    """Assess risk of a draft message."""
    draft_text = request.get("draft_text", "")
    masked_text_input = request.get("masked_text")
    masked_history_input = request.get("masked_history")
    session_id = request.get("session_id", 1)  # Scenario number (1, 2, or 3)
    participant_prolific_id = request.get("participant_prolific_id")
    
    logger.info("[RISK] assess_risk endpoint called with session_id=%s, draft_len=%d", 
                session_id, len(draft_text))
    
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
            "Explanation_NIST": "No PII detected; skipping LLM assessment.",
            "safer_rewrite": draft_text,
            "show_warning": False,
            "primary_risk_factors": [],
            "Reasoning": "",
            "Thought_Summary": "",
            # Backward-compatible keys
            "explanation": "No PII detected; skipping LLM assessment.",
            "reasoning_steps": "",
            "output_1": {
                "pii_sensitivity": {"level": "", "explanation": ""},
                "contextual_necessity": {"level": "", "explanation": ""},
                "intent_trajectory": {"level": "", "explanation": ""},
                "psychological_pressure": {"level": "", "explanation": ""},
                "identity_trust_signals": {"flags": [], "explanation": ""}
            },
            "output_2": {
                "original_user_message": draft_text,
                "risk_level": "LOW",
                "primary_risk_factors": [],
                "Explanation_NIST": "No PII detected; skipping LLM assessment.",
                "Reasoning": "",
                # Backward-compatible keys
                "explanation": "No PII detected; skipping LLM assessment.",
                "reasoning_steps": "",
                "rewrite": draft_text
            }
        }

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
    
    return {
        "risk_level": result["risk_level"],
        "Explanation_NIST": result["explanation"],
        "safer_rewrite": result["safer_rewrite"],
        "show_warning": result["show_warning"],
        "primary_risk_factors": result.get("primary_risk_factors", []),
        "Reasoning": result.get("reasoning_steps", ""),
        "Thought_Summary": result.get("thought_summary", ""),
        # Backward-compatible keys
        "explanation": result["explanation"],
        "reasoning_steps": result.get("reasoning_steps", ""),
        "output_1": result.get("output_1", {}),
        "output_2": result.get("output_2", {})
    }
