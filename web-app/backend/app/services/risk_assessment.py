"""
Risk assessment pipeline service.
"""
import logging
import json
import os
import re
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime
from app.utils import get_singapore_time

logger = logging.getLogger(__name__)


class RiskAssessmentService:
    """Service for risk assessment using an LLM provider."""
    
    def __init__(self, llm_service):
        """Initialize risk assessment service."""
        self.llm = llm_service
        self._prompt_template = None
    
    def _load_template(self, filename: str) -> str:
        """Load prompt template from file."""
        template_path = Path(__file__).parent.parent.parent / "assets" / filename
        if not template_path.exists():
            # Fallback to current directory
            template_path = Path(__file__).parent.parent / "assets" / filename
        if not template_path.exists():
            raise FileNotFoundError(f"Template not found: {filename}")
        
        return template_path.read_text()
    
    def _get_prompt_template(self) -> str:
        """Get prompt template (cached)."""
        if self._prompt_template is None:
            self._prompt_template = self._load_template("prompt.md")
        return self._prompt_template
    
    def _format_conversation_history_as_json(
        self,
        messages: List[Any]
    ) -> str:
        """Format conversation history as JSON for the prompt."""
        try:
            normalized = [
                msg.model_dump() if hasattr(msg, "model_dump") else msg
                for msg in messages
            ]
            return json.dumps(normalized, ensure_ascii=True, indent=2)
        except TypeError:
            return json.dumps(str(messages), ensure_ascii=True)

    def _build_assessment_prompt(
        self,
        prompt_template: str,
        history_json: str,
        current_user_message: str
    ) -> str:
        """
        Build the final prompt text with required inputs.
        Supports legacy templates with {history}/{input} placeholders and
        current templates that only describe expected inputs.
        """
        prompt = prompt_template
        injected = False

        if "{history}" in prompt:
            prompt = prompt.replace("{history}", history_json)
            injected = True
        if "{input}" in prompt:
            prompt = prompt.replace("{input}", current_user_message)
            injected = True

        if injected:
            return prompt

        # Current prompt.md does not contain placeholders, so append
        # explicit concrete inputs in a deterministic format.
        input_block = (
            "\n\n## Concrete Inputs\n"
            "Conversation_History_JSON:\n"
            "```json\n"
            f"{history_json}\n"
            "```\n\n"
            "Current_User_Message:\n"
            "```text\n"
            f"{current_user_message}\n"
            "```"
        )
        return f"{prompt}{input_block}"

    def _get_value(self, data: Any, keys: List[str], default: Any = None) -> Any:
        """Read a value from dict-like payloads using resilient key variants."""
        if not isinstance(data, dict):
            return default
        for key in keys:
            if key in data:
                return data[key]
        lowered_map = {str(k).lower(): v for k, v in data.items()}
        for key in keys:
            if key.lower() in lowered_map:
                return lowered_map[key.lower()]
        canonical_map = {self._canonical_key(k): v for k, v in data.items()}
        for key in keys:
            canonical_key = self._canonical_key(key)
            if canonical_key in canonical_map:
                return canonical_map[canonical_key]
        return default

    def _canonical_key(self, value: Any) -> str:
        """Normalize variant key casing/separators to a stable token."""
        return re.sub(r"[^a-z0-9]", "", str(value).lower())

    def _ensure_list(self, value: Any) -> List[Any]:
        """Normalize possible list-like values to a list."""
        if isinstance(value, list):
            return value
        if value is None:
            return []
        return [value]

    def _normalize_risk_level(self, value: Any) -> str:
        """Normalize model risk labels to LOW|MODERATE|HIGH."""
        normalized = str(value or "").strip().upper()
        if normalized in {"MEDIUM", "MODERATE"}:
            return "MODERATE"
        if normalized == "HIGH":
            return "HIGH"
        return "LOW"

    def _save_output_2(
        self,
        output: Dict[str, Any],
        session_id: Optional[int],
        prolific_id: Optional[str] = None
    ) -> None:
        """Persist OUTPUT 2 payloads under web-app/backend/app/data/llm_outputs (mounted volume)."""
        try:
            # Save to /app/app/data/llm_outputs (inside the app directory, maps to ./backend/app/data/llm_outputs)
            # The docker-compose mounts ./backend/data:/app/data, but the app code is in /app/app/
            # So we should save to /app/data/llm_outputs which maps to backend/data/llm_outputs
            base_dir = Path("/app/data/llm_outputs")
            
            # Fallback: try relative path from app directory
            if not base_dir.parent.exists():
                app_data_dir = Path(__file__).resolve().parent.parent / "data" / "llm_outputs"
                if app_data_dir.parent.exists():
                    base_dir = app_data_dir
                    logger.info("[LLM] Using fallback path: %s", base_dir)
                else:
                    # Last fallback: current working directory
                    base_dir = Path("/tmp/llm_outputs")
                    logger.warning("[LLM] Using /tmp fallback path: %s", base_dir)
            
            # Ensure directory exists
            base_dir.mkdir(parents=True, exist_ok=True)
            logger.info("[LLM] Output directory: %s (exists=%s, writable=%s)", 
                       base_dir, base_dir.exists(), os.access(base_dir, os.W_OK))
            
            now_sgt = get_singapore_time()
            timestamp = f"{now_sgt.strftime('%d%m%y_%H%M_%S')}_{now_sgt.microsecond // 1000:03d}"
            prolific_tag = self._safe_filename_part(prolific_id) if prolific_id else "ProlificIDUnknown"
            scenario_tag = f"Scenario{session_id}" if session_id is not None else "ScenarioUnknown"
            filename = f"{prolific_tag}_{scenario_tag}_{timestamp}.json"
            output_path = base_dir / filename

            # Keep deterministic naming while still avoiding accidental overwrite.
            if output_path.exists():
                suffix = 2
                while True:
                    candidate = base_dir / f"{prolific_tag}_{scenario_tag}_{timestamp}_{suffix}.json"
                    if not candidate.exists():
                        output_path = candidate
                        break
                    suffix += 1
            
            # Write output
            output_json = json.dumps(output, ensure_ascii=True, indent=2)
            output_path.write_text(output_json, encoding='utf-8')
            logger.info("[LLM] Output saved successfully to %s (size=%d bytes, exists=%s)", 
                       output_path, len(output_json), output_path.exists())
        except Exception as e:
            logger.error("[LLM] Failed to save output: %s", e, exc_info=True)
            # Don't raise - logging is enough, don't break the flow

    def _safe_filename_part(self, value: str) -> str:
        """Convert prolific id to a filesystem-safe fragment."""
        cleaned = re.sub(r"[^A-Za-z0-9._-]", "_", value.strip())
        cleaned = re.sub(r"_+", "_", cleaned)
        return cleaned[:80] if cleaned else "unknown"

    def _contains_mask_tokens(self, text: str) -> bool:
        """Detect placeholder masks like [LOCATION_CITY] in model output."""
        if not text:
            return False
        return bool(re.search(r"\[[A-Z0-9_]+\]", text))

    def _fallback_reasoning(self) -> str:
        """One-line user-facing reason when LLM output is unavailable/incomplete."""
        return "Sensitive details were detected, so this rewrite keeps your intent while sharing less."

    def _fallback_conversational_rewrite(
        self,
        draft_text: str,
        masked_draft: Optional[str] = None
    ) -> str:
        """
        Create a conversational privacy-preserving fallback rewrite.
        Never return raw masked placeholders to users.
        """
        source = (masked_draft or draft_text or "").lower()
        has_location = "location" in source or "address" in source or "where" in source
        has_phone = "phone" in source or "mobile" in source or "number" in source
        has_email = "email" in source or "mail" in source
        has_dob = "birth" in source or "dob" in source or "age" in source
        has_financial = "bank" in source or "card" in source or "account" in source or "payment" in source
        has_id = "id" in source or "passport" in source or "nric" in source or "license" in source

        sensitive_hits = sum(
            1 for flag in [has_location, has_phone, has_email, has_dob, has_financial, has_id] if flag
        )
        if sensitive_hits >= 2:
            return "I’m not comfortable sharing those personal details right now, but I can continue without them."
        if has_location:
            return "I’m not comfortable sharing my exact location right now."
        if has_phone:
            return "I’m not comfortable sharing my phone number right now."
        if has_email:
            return "I’d prefer not to share my email address right now."
        if has_dob:
            return "I’d prefer not to share my date of birth."
        if has_financial:
            return "I can’t share financial account details here."
        if has_id:
            return "I’m not comfortable sharing my ID details here."
        return "I’d prefer to keep that personal information private for now."

    def _normalize_risk_payload(
        self,
        raw: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Normalize model output into canonical Output_1/Output_2 schema.
        This stabilizes downstream parsing and saved output files.
        """
        output_1_raw = self._get_value(raw, ["Output_1", "output_1", "Output1"], {})
        output_2_raw = self._get_value(raw, ["Output_2", "output_2", "Output2"], raw)
        if not isinstance(output_1_raw, dict):
            output_1_raw = {}
        if not isinstance(output_2_raw, dict):
            output_2_raw = {}

        linkability_risk_raw = self._get_value(
            output_1_raw,
            ["Linkability_Risk", "linkability_risk", "linkabilityRisk"],
            {},
        )
        authentication_baiting_raw = self._get_value(
            output_1_raw,
            ["Authentication_Baiting", "authentication_baiting", "authenticationBaiting"],
            {},
        )
        contextual_alignment_raw = self._get_value(
            output_1_raw,
            ["Contextual_Alignment", "contextual_alignment", "contextualAlignment"],
            {},
        )
        platform_trust_obligation_raw = self._get_value(
            output_1_raw,
            ["Platform_Trust_Obligation", "platform_trust_obligation", "platformTrustObligation"],
            {},
        )
        psychological_pressure_raw = self._get_value(output_1_raw, ["Psychological_Pressure", "psychological_pressure"], {})

        normalized_output_1 = {
            "Linkability_Risk": {
                "Level": self._get_value(linkability_risk_raw, ["Level", "level"], ""),
                "Explanation": self._get_value(linkability_risk_raw, ["Explanation", "explanation"], ""),
            },
            "Authentication_Baiting": {
                "Level": self._get_value(authentication_baiting_raw, ["Level", "level"], ""),
                "Explanation": self._get_value(authentication_baiting_raw, ["Explanation", "explanation"], ""),
            },
            "Contextual_Alignment": {
                "Level": self._get_value(contextual_alignment_raw, ["Level", "level"], ""),
                "Explanation": self._get_value(contextual_alignment_raw, ["Explanation", "explanation"], ""),
            },
            "Platform_Trust_Obligation": {
                "Level": self._get_value(platform_trust_obligation_raw, ["Level", "level"], ""),
                "Explanation": self._get_value(platform_trust_obligation_raw, ["Explanation", "explanation"], ""),
            },
            "Psychological_Pressure": {
                "Level": self._get_value(psychological_pressure_raw, ["Level", "level"], ""),
                "Explanation": self._get_value(psychological_pressure_raw, ["Explanation", "explanation"], ""),
            },
        }

        reasoning = self._get_value(
            output_2_raw,
            ["Reasoning", "reasoning"],
            "",
        )
        normalized_output_2 = {
            "Original_User_Message": self._get_value(
                output_2_raw,
                ["Original_User_Message", "original_user_message", "originalUserMessage"],
                "",
            ),
            "Risk_Level": self._normalize_risk_level(
                self._get_value(output_2_raw, ["Risk_Level", "risk_level", "riskLevel"], "LOW")
            ),
            "Primary_Risk_Factors": self._ensure_list(
                self._get_value(
                    output_2_raw,
                    ["Primary_Risk_Factors", "primary_risk_factors", "primaryRiskFactors"],
                    [],
                )
            ),
            "Reasoning": reasoning,
            "Rewrite": self._get_value(
                output_2_raw,
                ["Rewrite", "rewrite"],
                "",
            ),
        }

        return {
            "Output_1": normalized_output_1,
            "Output_2": normalized_output_2,
        }
    
    def assess_risk(
        self,
        draft_text: str,
        conversation_history: List[Any],
        masked_draft: Optional[str] = None,
        masked_history: Optional[List[Any]] = None,
        session_id: Optional[int] = None,
        prolific_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Assess risk of a draft message.
        
        Args:
            draft_text: Original draft text
            conversation_history: List of previous messages
            masked_draft: Masked draft (if already processed)
            masked_history: Masked history (if already processed)
        
        Returns:
            Risk assessment result with risk_level, reasoning, safer_rewrite, etc.
        """
        try:
            # ALWAYS use masked version if provided - this is critical for PII privacy
            # The LLM should only see masked PII, not the actual PII
            if masked_draft:
                draft = masked_draft
                logger.info("[LLM] Using MASKED draft for LLM (privacy-protected): draft_len=%d, masked_len=%d", 
                           len(draft_text), len(draft))
            else:
                draft = draft_text
                logger.warning("[LLM] No masked draft provided, using original (may contain PII): draft_len=%d", 
                              len(draft))
            
            history = masked_history if masked_history else conversation_history
            
            # Format history as JSON
            history_json = self._format_conversation_history_as_json(history)
            
            # Step 1: Load and fill prompt template with concrete inputs
            prompt_template = self._get_prompt_template()
            first_prompt = self._build_assessment_prompt(
                prompt_template=prompt_template,
                history_json=history_json,
                current_user_message=draft,
            )
            
            logger.info(
                "[LLM] Prompt prepared: history_msgs=%d, input_len=%d, prompt_len=%d",
                len(history),
                len(draft),
                len(first_prompt),
            )
            # Step 2: Call LLM API for Output 2
            logger.info("Calling LLM API for risk assessment")
            risk_result = self.llm.generate_json_content(first_prompt)
            normalized_result = self._normalize_risk_payload(
                risk_result if isinstance(risk_result, dict) else {}
            )
            self._save_output_2(normalized_result, session_id=session_id, prolific_id=prolific_id)

            output_1 = normalized_result.get("Output_1", {})
            output_2 = normalized_result.get("Output_2", {})

            risk_level = self._normalize_risk_level(
                self._get_value(output_2, ["Risk_Level", "risk_level", "riskLevel"], "LOW")
            )
            show_warning = risk_level in {"MODERATE", "HIGH"}
            reasoning = self._get_value(
                output_2,
                ["Reasoning", "reasoning"],
                ""
            )
            original_user_message = self._get_value(
                output_2,
                ["Original_User_Message", "original_user_message", "originalUserMessage"],
                ""
            )
            primary_risk_factors = self._ensure_list(
                self._get_value(
                    output_2,
                    ["Primary_Risk_Factors", "primary_risk_factors"],
                    []
                )
            )
            
            # Get safer rewrite from LLM response
            safer_rewrite = self._get_value(
                output_2,
                ["Rewrite", "rewrite"],
                ""
            )
            if not safer_rewrite or self._contains_mask_tokens(safer_rewrite):
                safer_rewrite = self._fallback_conversational_rewrite(draft_text=draft_text, masked_draft=masked_draft)
            if not reasoning:
                reasoning = self._fallback_reasoning()

            linkability_risk = self._get_value(output_1, ["Linkability_Risk", "linkability_risk"], {})
            authentication_baiting = self._get_value(output_1, ["Authentication_Baiting", "authentication_baiting"], {})
            contextual_alignment = self._get_value(output_1, ["Contextual_Alignment", "contextual_alignment"], {})
            platform_trust_obligation = self._get_value(
                output_1,
                ["Platform_Trust_Obligation", "platform_trust_obligation"],
                {},
            )
            psychological_pressure = self._get_value(output_1, ["Psychological_Pressure", "psychological_pressure"], {})
            
            return {
                "risk_level": risk_level,
                "safer_rewrite": safer_rewrite,
                "show_warning": show_warning,
                "reasoning": reasoning,
                "primary_risk_factors": primary_risk_factors,
                "output_1": {
                    "linkability_risk": {
                        "level": self._get_value(linkability_risk, ["Level", "level"], ""),
                        "explanation": self._get_value(linkability_risk, ["Explanation", "explanation"], "")
                    },
                    "authentication_baiting": {
                        "level": self._get_value(authentication_baiting, ["Level", "level"], ""),
                        "explanation": self._get_value(authentication_baiting, ["Explanation", "explanation"], "")
                    },
                    "contextual_alignment": {
                        "level": self._get_value(contextual_alignment, ["Level", "level"], ""),
                        "explanation": self._get_value(contextual_alignment, ["Explanation", "explanation"], "")
                    },
                    "platform_trust_obligation": {
                        "level": self._get_value(platform_trust_obligation, ["Level", "level"], ""),
                        "explanation": self._get_value(platform_trust_obligation, ["Explanation", "explanation"], "")
                    },
                    "psychological_pressure": {
                        "level": self._get_value(psychological_pressure, ["Level", "level"], ""),
                        "explanation": self._get_value(psychological_pressure, ["Explanation", "explanation"], "")
                    }
                },
                "output_2": {
                    "original_user_message": original_user_message,
                    "risk_level": risk_level,
                    "primary_risk_factors": primary_risk_factors,
                    "reasoning": reasoning,
                    "rewrite": safer_rewrite
                }
            }
        
        except Exception as e:
            logger.error(f"Risk assessment error: {e}", exc_info=True)
            # Keep warning flow active when PII was already detected, even if LLM is unavailable.
            fallback_rewrite = self._fallback_conversational_rewrite(draft_text=draft_text, masked_draft=masked_draft)
            fallback_risk = "MODERATE" if masked_draft else "LOW"
            fallback_reasoning = self._fallback_reasoning()
            fallback_linkability_level = "MODERATE" if masked_draft else "LOW"
            fallback_output_1 = {
                "linkability_risk": {
                    "level": fallback_linkability_level,
                    "explanation": "Estimated fallback because risk model output was unavailable."
                },
                "authentication_baiting": {
                    "level": "UNKNOWN",
                    "explanation": "Could not evaluate auth-baiting due temporary model unavailability."
                },
                "contextual_alignment": {
                    "level": "UNKNOWN",
                    "explanation": "Could not evaluate context due temporary model unavailability."
                },
                "platform_trust_obligation": {
                    "level": "UNKNOWN",
                    "explanation": "Could not evaluate platform trust due temporary model unavailability."
                },
                "psychological_pressure": {
                    "level": "UNKNOWN",
                    "explanation": "Could not evaluate pressure due temporary model unavailability."
                }
            }
            fallback_output_2 = {
                "original_user_message": draft_text,
                "risk_level": fallback_risk,
                "primary_risk_factors": [],
                "reasoning": fallback_reasoning,
                "rewrite": fallback_rewrite
            }
            try:
                self._save_output_2(
                    {
                        "Output_1": {
                            "Linkability_Risk": {
                                "Level": fallback_output_1["linkability_risk"]["level"],
                                "Explanation": fallback_output_1["linkability_risk"]["explanation"],
                            },
                            "Authentication_Baiting": {
                                "Level": fallback_output_1["authentication_baiting"]["level"],
                                "Explanation": fallback_output_1["authentication_baiting"]["explanation"],
                            },
                            "Contextual_Alignment": {
                                "Level": fallback_output_1["contextual_alignment"]["level"],
                                "Explanation": fallback_output_1["contextual_alignment"]["explanation"],
                            },
                            "Platform_Trust_Obligation": {
                                "Level": fallback_output_1["platform_trust_obligation"]["level"],
                                "Explanation": fallback_output_1["platform_trust_obligation"]["explanation"],
                            },
                            "Psychological_Pressure": {
                                "Level": fallback_output_1["psychological_pressure"]["level"],
                                "Explanation": fallback_output_1["psychological_pressure"]["explanation"],
                            },
                        },
                        "Output_2": {
                            "Original_User_Message": draft_text,
                            "Risk_Level": fallback_risk,
                            "Primary_Risk_Factors": [],
                            "Reasoning": fallback_reasoning,
                            "Rewrite": fallback_rewrite,
                        },
                        "error": str(e),
                    },
                    session_id=session_id,
                    prolific_id=prolific_id,
                )
            except Exception:
                logger.warning("[LLM] Failed to persist fallback output payload", exc_info=True)
            return {
                "risk_level": fallback_risk,
                "safer_rewrite": fallback_rewrite,
                "show_warning": fallback_risk in {"MODERATE", "HIGH"},
                "primary_risk_factors": [],
                "reasoning": fallback_reasoning,
                "output_1": fallback_output_1,
                "output_2": fallback_output_2,
                "error": str(e)
            }
    
