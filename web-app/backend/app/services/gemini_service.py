"""
Gemini API service abstraction layer.
"""
import json
import logging
import re
import time
from datetime import datetime
from typing import Optional, Dict, Any, List
from urllib.parse import quote

import requests
from app.config import settings

logger = logging.getLogger(__name__)


def _timestamp() -> str:
    """Return current timestamp in ISO format."""
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"


_THINKING_LEVEL_VALUES = {"minimal", "low", "medium", "high"}
_THINKING_BUDGET_WORD_MAP = {
    "off": 0,
    "none": 0,
    "disabled": 0,
    "zero": 0,
    "dynamic": -1,
    "default": -1,
    "auto": -1,
    "low": 1024,
    "medium": 8192,
    "high": 24576,
}


def _normalize_model_name(model_name: str) -> str:
    normalized = (model_name or "").strip().lower()
    if normalized.startswith("models/"):
        return normalized[len("models/"):]
    return normalized


def _is_gemini_3_model(model_name: str) -> bool:
    normalized = _normalize_model_name(model_name)
    return normalized.startswith("gemini-3")


def _budget_from_thinking_power(raw_value: Any, default_value: int = -1) -> int:
    """Parse integer thinking budget from either integer-like values or words."""
    if raw_value is None:
        return int(default_value)

    normalized = str(raw_value).strip().lower()
    if not normalized:
        return int(default_value)

    if re.fullmatch(r"-?\d+", normalized):
        return int(normalized)

    if normalized in _THINKING_BUDGET_WORD_MAP:
        return int(_THINKING_BUDGET_WORD_MAP[normalized])

    logger.warning("Unknown thinking power value '%s'; using default=%s", raw_value, default_value)
    return int(default_value)


def _level_from_thinking_power(raw_value: Any) -> Optional[str]:
    """
    Convert env thinking power into Gemini 3 thinkingLevel.
    Returns None to defer to model default behavior.
    """
    if raw_value is None:
        return None

    normalized = str(raw_value).strip().lower()
    if not normalized:
        return None

    if normalized in _THINKING_LEVEL_VALUES:
        return normalized

    if normalized in {"off", "none", "disabled", "zero"}:
        return "minimal"

    if normalized in {"dynamic", "default", "auto", "-1"}:
        return None

    if re.fullmatch(r"-?\d+", normalized):
        value = int(normalized)
        if value <= 0:
            return "minimal"
        if value <= 4096:
            return "low"
        if value <= 12288:
            return "medium"
        return "high"

    logger.warning("Unknown Gemini 3 thinking level value '%s'; using model default", raw_value)
    return None


class GeminiService:
    """Service for interacting with Gemini using generateContent REST API."""

    def __init__(
        self,
        timeout_seconds: Optional[int] = None,
        max_attempts: Optional[int] = None,
    ):
        self.api_key = settings.GEMINI_API_KEY
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY environment variable is required")

        self.model = settings.GEMINI_FIRST_MODEL
        self.first_model = settings.GEMINI_FIRST_MODEL
        if not self.first_model:
            raise ValueError("GEMINI_FIRST_MODEL environment variable is required")
        self.second_model = settings.GEMINI_SECOND_MODEL
        self.first_model_timeout_seconds = int(
            timeout_seconds if timeout_seconds is not None else settings.FIRST_MODEL_TIMEOUT_SECONDS
        )
        primary_attempts = max_attempts if max_attempts is not None else settings.FIRST_MODEL_MAX_ATTEMPTS
        self.primary_max_attempts = max(1, int(primary_attempts))
        self.second_model_timeout_seconds = int(settings.SECOND_MODEL_TIMEOUT_SECONDS)
        self.second_model_max_attempts = max(1, int(settings.SECOND_MODEL_MAX_ATTEMPTS))
        self.first_model_thinking_power = settings.FIRST_MODEL_THINKING_POWER
        self.second_model_thinking_power = settings.SECOND_MODEL_THINKING_POWER
        self.include_thoughts = settings.GEMINI_INCLUDE_THOUGHTS
        self.base_url = "https://generativelanguage.googleapis.com"
        self.api_version = "v1beta"
        self._last_thought_summaries: List[str] = []
        self._last_model_used: Optional[str] = None
        self._last_usage_metadata: Dict[str, Optional[int | str]] = {
            "output_id": None,
            "total_tokens": None,
            "input_tokens": None,
        }

    def _build_prompt_text(
        self,
        prompt: str,
        context: Optional[Dict[str, Any]]
    ) -> str:
        if not context:
            return prompt
        context_json = json.dumps(context, ensure_ascii=False, indent=2)
        return f"{prompt}\n\nConversation context JSON:\n```json\n{context_json}\n```"

    def _model_resource(self, model_name: str) -> str:
        """Normalize model id into models/{model} resource format."""
        resource = model_name.strip()
        if not resource.startswith("models/"):
            resource = f"models/{resource}"
        return quote(resource, safe="/._-")

    def _build_thinking_config(self, model_name: str, thinking_power: Any) -> Dict[str, Any]:
        """Build model-specific thinking config."""
        config: Dict[str, Any] = {"includeThoughts": self.include_thoughts}

        # Gemini 3 models prefer thinkingLevel (minimal|low|medium|high).
        if _is_gemini_3_model(model_name):
            level = _level_from_thinking_power(thinking_power)
            if level:
                config["thinkingLevel"] = level
            return config

        # Other models use thinkingBudget integers.
        budget = _budget_from_thinking_power(thinking_power, default_value=-1)
        config["thinkingBudget"] = budget
        return config

    def _build_request_payload(self, model_name: str, content_text: str, thinking_power: Any) -> Dict[str, Any]:
        """Build generateContent request payload with model-specific thinking configuration."""
        return {
            "contents": [
                {
                    "parts": [
                        {"text": content_text}
                    ]
                }
            ],
            "generationConfig": {
                "thinkingConfig": self._build_thinking_config(
                    model_name=model_name,
                    thinking_power=thinking_power,
                )
            },
        }

    def _generate_content_via_rest(
        self,
        model_name: str,
        content_text: str,
        timeout_seconds: int,
        thinking_power: Any,
    ) -> Dict[str, Any]:
        """Call Gemini generateContent endpoint directly."""
        model_resource = self._model_resource(model_name)
        url = f"{self.base_url}/{self.api_version}/{model_resource}:generateContent"
        payload = self._build_request_payload(
            model_name=model_name,
            content_text=content_text,
            thinking_power=thinking_power,
        )
        headers = {
            "Content-Type": "application/json",
            "X-goog-api-key": self.api_key,
        }
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    def _resolve_status_code(self, error: Exception) -> Optional[int]:
        status = getattr(error, "status_code", None)
        if status is None and hasattr(error, "response") and getattr(error, "response", None) is not None:
            status = getattr(error.response, "status_code", None)
        if status is None:
            status = getattr(error, "code", None)
        return status

    def _call_model_with_retries(
        self,
        model_name: str,
        content_text: str,
        thinking_power: Any,
        timeout_seconds: int,
        max_attempts: int,
    ) -> str:
        attempts = max(1, int(max_attempts))
        for attempt in range(1, attempts + 1):
            try:
                call_start = _timestamp()
                logger.info(
                    "[LLM] Request started at %s (provider=gemini, model=%s, attempt=%d/%d, thinking_power=%s, timeout=%ss)",
                    call_start,
                    model_name,
                    attempt,
                    attempts,
                    thinking_power,
                    timeout_seconds,
                )

                response = self._generate_content_via_rest(
                    model_name=model_name,
                    content_text=content_text,
                    timeout_seconds=timeout_seconds,
                    thinking_power=thinking_power,
                )
                self._last_thought_summaries = self._extract_thought_summaries(response)

                call_end = _timestamp()
                logger.info(
                    "[LLM] Response received at %s (provider=gemini, model=%s, attempt=%d/%d)",
                    call_end,
                    model_name,
                    attempt,
                    attempts,
                )
                if self._last_thought_summaries:
                    logger.info("[LLM] Thought summaries captured: %d", len(self._last_thought_summaries))

                self._last_model_used = model_name
                self._last_usage_metadata = self._extract_usage_metadata(response)
                return self._extract_text(response)
            except Exception as e:
                status = self._resolve_status_code(e)
                if attempt < attempts:
                    should_retry = status in (429, 500, 502, 503, 504) or status is None
                    if should_retry:
                        sleep_seconds = 2
                        logger.warning(
                            "Gemini request failed, retrying in %ss (model=%s, attempt=%d/%d): %s",
                            sleep_seconds,
                            model_name,
                            attempt,
                            attempts,
                            e,
                        )
                        time.sleep(sleep_seconds)
                        continue
                logger.error("Gemini API error (model=%s): %s", model_name, e)
                raise

    def _extract_text(self, response: Any) -> str:
        text = response.get("text") if isinstance(response, dict) else getattr(response, "text", None)
        if isinstance(text, str) and text.strip():
            return text.strip()

        candidates = response.get("candidates", []) if isinstance(response, dict) else (getattr(response, "candidates", None) or [])
        if candidates:
            first = candidates[0]
            content = first.get("content", {}) if isinstance(first, dict) else getattr(first, "content", None)
            parts = content.get("parts", []) if isinstance(content, dict) else (getattr(content, "parts", None) or [])
            collected = []
            for part in parts:
                is_thought = part.get("thought") if isinstance(part, dict) else getattr(part, "thought", None)
                if is_thought:
                    # Keep thought summaries separate from the final assistant text.
                    continue
                part_text = getattr(part, "text", None)
                if part_text is None and isinstance(part, dict):
                    part_text = part.get("text")
                if part_text:
                    collected.append(str(part_text))
            joined = "".join(collected).strip()
            if joined:
                return joined

        raise ValueError("Gemini response missing text content")

    def _extract_thought_summaries(self, response: Any) -> List[str]:
        """Collect thought-summary parts returned when include_thoughts is enabled."""
        summaries: List[str] = []
        candidates = response.get("candidates", []) if isinstance(response, dict) else (getattr(response, "candidates", None) or [])
        for candidate in candidates:
            content = candidate.get("content", {}) if isinstance(candidate, dict) else getattr(candidate, "content", None)
            parts = content.get("parts", []) if isinstance(content, dict) else (getattr(content, "parts", None) or [])
            for part in parts:
                is_thought = getattr(part, "thought", None)
                if is_thought is None and isinstance(part, dict):
                    is_thought = part.get("thought")
                if not is_thought:
                    continue

                part_text = getattr(part, "text", None)
                if part_text is None and isinstance(part, dict):
                    part_text = part.get("text")
                normalized = str(part_text or "").strip()
                if normalized:
                    summaries.append(normalized)

        # Preserve order but de-duplicate exact repeats.
        deduped: List[str] = []
        seen = set()
        for item in summaries:
            if item in seen:
                continue
            deduped.append(item)
            seen.add(item)
        return deduped

    def get_last_thought_summaries(self) -> List[str]:
        """Return thought summaries from the most recent request in this service instance."""
        return list(self._last_thought_summaries)

    def get_last_model_used(self) -> Optional[str]:
        """Return model id used by the most recent successful request."""
        return self._last_model_used

    def get_last_usage_metadata(self) -> Dict[str, Optional[int | str]]:
        """Return output id and token usage from the most recent successful request."""
        return dict(self._last_usage_metadata)

    def _extract_usage_metadata(self, response: Any) -> Dict[str, Optional[int | str]]:
        if not isinstance(response, dict):
            return {"output_id": None, "total_tokens": None, "input_tokens": None}

        usage = response.get("usageMetadata", {}) or {}
        total_tokens = usage.get("totalTokenCount")
        input_tokens = usage.get("promptTokenCount")
        output_id = response.get("responseId") or response.get("response_id")

        try:
            total_tokens = int(total_tokens) if total_tokens is not None else None
        except (TypeError, ValueError):
            total_tokens = None
        try:
            input_tokens = int(input_tokens) if input_tokens is not None else None
        except (TypeError, ValueError):
            input_tokens = None

        return {
            "output_id": str(output_id) if output_id is not None else None,
            "total_tokens": total_tokens,
            "input_tokens": input_tokens,
        }

    def generate_content(
        self,
        prompt: str,
        context: Optional[Dict[str, Any]] = None,
        model: Optional[str] = None
    ) -> str:
        candidate = model or self.first_model
        content_text = self._build_prompt_text(prompt, context)
        self._last_thought_summaries = []
        self._last_model_used = None
        self._last_usage_metadata = {"output_id": None, "total_tokens": None, "input_tokens": None}

        try:
            return self._call_model_with_retries(
                model_name=candidate,
                content_text=content_text,
                thinking_power=self.first_model_thinking_power,
                timeout_seconds=self.first_model_timeout_seconds,
                max_attempts=self.primary_max_attempts,
            )
        except Exception as primary_error:
            if not self.second_model:
                raise

            sleep_seconds = 2
            logger.warning(
                "[LLM] Primary model failed (model=%s). Falling back to secondary model=%s in %ss. Error=%s",
                candidate,
                self.second_model,
                sleep_seconds,
                primary_error,
            )
            time.sleep(sleep_seconds)
            return self._call_model_with_retries(
                model_name=self.second_model,
                content_text=content_text,
                thinking_power=self.second_model_thinking_power,
                timeout_seconds=self.second_model_timeout_seconds,
                max_attempts=self.second_model_max_attempts,
            )

    def generate_json_content(
        self,
        prompt: str,
        context: Optional[Dict[str, Any]] = None,
        model: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            text = self.generate_content(prompt, context=context, model=model)

            if "```json" in text:
                start = text.find("```json") + 7
                end = text.find("```", start)
                text = text[start:end].strip()
            elif "```" in text:
                start = text.find("```") + 3
                end = text.find("```", start)
                text = text[start:end].strip()

            return json.loads(text)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse JSON response: %s", e)
            logger.error("Response text: %s", text)
            raise ValueError(f"Invalid JSON response from Gemini: {e}")
        except Exception as e:
            logger.error("Error generating JSON content: %s", e)
            raise
