"""
Gemini API service abstraction layer.
"""
import json
import logging
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


class GeminiService:
    """Service for interacting with Gemini using generateContent REST API."""

    def __init__(
        self,
        timeout_seconds: Optional[int] = None,
        max_attempts: Optional[int] = None,
    ):
        self.api_key = settings.GOOGLE_API_KEY or settings.GEMINI_API_KEY
        if not self.api_key:
            raise ValueError("GOOGLE_API_KEY or GEMINI_API_KEY environment variable is required")
        if settings.GOOGLE_API_KEY and settings.GEMINI_API_KEY:
            logger.info("Both GOOGLE_API_KEY and GEMINI_API_KEY are set; using GOOGLE_API_KEY.")

        self.model = settings.GEMINI_MODEL_NAME
        self.timeout = int(timeout_seconds if timeout_seconds is not None else settings.GEMINI_TIMEOUT_SECONDS)
        self.max_attempts = max(1, int(max_attempts if max_attempts is not None else settings.GEMINI_MAX_ATTEMPTS))
        self.thinking_level = settings.GEMINI_THINKING_LEVEL
        self.include_thoughts = settings.GEMINI_INCLUDE_THOUGHTS
        self.base_url = "https://generativelanguage.googleapis.com"
        self.api_version = "v1beta"
        self._last_thought_summaries: List[str] = []

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

    def _build_request_payload(self, content_text: str) -> Dict[str, Any]:
        """Build generateContent request payload with thinking configuration."""
        return {
            "contents": [
                {
                    "parts": [
                        {"text": content_text}
                    ]
                }
            ],
            "generationConfig": {
                "thinkingConfig": {
                    "thinkingLevel": self.thinking_level,
                    "includeThoughts": self.include_thoughts,
                }
            },
        }

    def _generate_content_via_rest(self, model_name: str, content_text: str) -> Dict[str, Any]:
        """Call Gemini generateContent endpoint directly."""
        model_resource = self._model_resource(model_name)
        url = f"{self.base_url}/{self.api_version}/{model_resource}:generateContent"
        payload = self._build_request_payload(content_text)
        headers = {
            "Content-Type": "application/json",
            "X-goog-api-key": self.api_key,
        }
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

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

    def generate_content(
        self,
        prompt: str,
        context: Optional[Dict[str, Any]] = None,
        model: Optional[str] = None
    ) -> str:
        candidate = model or self.model
        content_text = self._build_prompt_text(prompt, context)
        self._last_thought_summaries = []

        max_attempts = self.max_attempts
        for attempt in range(1, max_attempts + 1):
            try:
                call_start = _timestamp()
                logger.info(
                    "[LLM] Request started at %s (provider=gemini, model=%s, attempt=%d/%d)",
                    call_start,
                    candidate,
                    attempt,
                    max_attempts,
                )

                response = self._generate_content_via_rest(candidate, content_text)
                self._last_thought_summaries = self._extract_thought_summaries(response)

                call_end = _timestamp()
                logger.info(
                    "[LLM] Response received at %s (provider=gemini, model=%s, attempt=%d/%d)",
                    call_end,
                    candidate,
                    attempt,
                    max_attempts,
                )
                if self._last_thought_summaries:
                    logger.info("[LLM] Thought summaries captured: %d", len(self._last_thought_summaries))

                return self._extract_text(response)
            except Exception as e:
                status = getattr(e, "status_code", None)
                if status is None and hasattr(e, "response") and getattr(e, "response", None) is not None:
                    status = getattr(e.response, "status_code", None)
                if status is None:
                    status = getattr(e, "code", None)
                if attempt < max_attempts:
                    should_retry = status in (429, 500, 502, 503, 504) or status is None
                    if should_retry:
                        sleep_seconds = 2
                        logger.warning(
                            "Gemini request failed, retrying in %ss (attempt=%d/%d): %s",
                            sleep_seconds,
                            attempt,
                            max_attempts,
                            e,
                        )
                        time.sleep(sleep_seconds)
                        continue
                logger.error("Gemini API error (model=%s): %s", candidate, e)
                raise

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
