"""
GLiNER service implementation for PII detection and masking.
Based on the gliner_chunking.ipynb notebook logic.
"""

import logging
import os
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass
from gliner import GLiNER
from transformers import AutoTokenizer
from nltk.tokenize import sent_tokenize
import nltk

logger = logging.getLogger(__name__)


@dataclass
class PiiSpan:
    """Represents a detected PII span."""
    start: int
    end: int
    label: str
    text: str


@dataclass
class MaskingResult:
    """Result of masking and chunking operation."""
    masked_text: str
    chunks: List[str]
    pii_spans: List[PiiSpan]


class GliNERService:
    """Service for GLiNER-based PII detection and masking."""
    
    # PII labels from the notebook
    PERSONAL_LABELS = [
        "name",
        "first name",
        "last name",
        "name medical professional",
        "dob",
        "age",
        "gender",
        "marital status"
    ]
    
    CONTACT_LABELS = [
        "email address",
        "phone number",
        "ip address",
        "url",
        "location address",
        "location street",
        "location city",
        "location state",
        "location country",
        "location zip"
    ]
    
    FINANCIAL_LABELS = [
        "account number",
        "bank account",
        "routing number",
        "credit card",
        "credit card expiration",
        "cvv",
        "ssn",
        "money"
    ]
    
    HEALTHCARE_LABELS = [
        "condition",
        "medical process",
        "drug",
        "dose",
        "blood type",
        "injury",
        "organization medical facility",
        "healthcare number",
        "medical code"
    ]
    
    ID_LABELS = [
        "passport number",
        "driver license",
        "username",
        "password",
        "vehicle id"
    ]
    
    def __init__(self, model_name: str | None = None):
        """Initialize GLiNER model and tokenizer."""
        self.model_name = model_name or os.getenv("GLINER_MODEL_NAME", "knowledgator/gliner-pii-base-v1.0")
        self.model: Optional[GLiNER] = None
        self.tokenizer: Optional[AutoTokenizer] = None
        self.labels = (
            self.PERSONAL_LABELS +
            self.CONTACT_LABELS +
            self.FINANCIAL_LABELS +
            self.HEALTHCARE_LABELS +
            self.ID_LABELS
        )
        self._initialized = False
        
    def _ensure_nltk_data(self):
        """Ensure NLTK punkt tokenizer data is available."""
        try:
            nltk.data.find('tokenizers/punkt')
        except LookupError:
            logger.info("Downloading NLTK punkt tokenizer...")
            nltk.download('punkt', quiet=True)
    
    def initialize(self):
        """Lazy initialization of model and tokenizer."""
        if self._initialized:
            return
        
        try:
            logger.info(f"Loading GLiNER model: {self.model_name}")
            try:
                self.model = GLiNER.from_pretrained(self.model_name, strict=False)
            except TypeError:
                self.model = GLiNER.from_pretrained(self.model_name)
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            self._ensure_nltk_data()
            self._initialized = True
            logger.info("GLiNER model loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load GLiNER model: {e}")
            raise
    
    def is_loaded(self) -> bool:
        """Check if model is loaded."""
        return self._initialized and self.model is not None
    
    def mask_and_chunk(
        self,
        text: str,
        max_tokens: int = 512
    ) -> MaskingResult:
        """
        Mask PII entities and chunk text.
        
        Args:
            text: Input text to process
            max_tokens: Maximum tokens per chunk
            
        Returns:
            MaskingResult with masked text, chunks, and PII spans
        """
        if not self.is_loaded():
            self.initialize()
        logger.info("GLiNER masking start (len=%s)", len(text))

        token_count = len(self.tokenizer.encode(text, add_special_tokens=False))
        pii_spans: List[PiiSpan] = []

        # Mirror notebook behavior: no chunking when input is within limit.
        if token_count <= max_tokens:
            masked_text, entities = self._redact_with_gliner(text)
            for ent in entities:
                pii_spans.append(
                    PiiSpan(
                        start=ent["start"],
                        end=ent["end"],
                        label=ent["label"],
                        text=text[ent["start"]:ent["end"]],
                    )
                )
            pii_spans.sort(key=lambda x: x.start)
            return MaskingResult(
                masked_text=masked_text,
                chunks=[masked_text] if masked_text else [],
                pii_spans=pii_spans,
            )

        # Mirror notebook behavior for long input:
        # sentence chunking (no overlap) -> per-chunk GLiNER redaction -> join.
        chunk_infos = self._chunk_sentences_with_metadata(text, max_tokens)
        redacted_chunks: List[str] = []
        for chunk_info in chunk_infos:
            redacted_chunk, entities = self._redact_with_gliner(chunk_info["text"])
            redacted_chunks.append(redacted_chunk)

            for ent in entities:
                global_span = self._map_chunk_entity_to_original(ent, chunk_info["segments"])
                if global_span is None:
                    continue
                start, end = global_span
                pii_spans.append(
                    PiiSpan(
                        start=start,
                        end=end,
                        label=ent["label"],
                        text=text[start:end],
                    )
                )

        pii_spans.sort(key=lambda x: x.start)
        return MaskingResult(
            masked_text=" ".join(redacted_chunks),
            chunks=redacted_chunks,
            pii_spans=pii_spans,
        )

    def _redact_with_gliner(self, text_chunk: str) -> Tuple[str, List[Dict[str, Any]]]:
        """Notebook-equivalent GLiNER redaction for a text chunk."""
        entities = self.model.predict_entities(text_chunk, self.labels)
        redacted = text_chunk
        for ent in sorted(entities, key=lambda x: x["start"], reverse=True):
            tag = f"[{ent['label'].upper().replace(' ', '_')}]"
            redacted = redacted[:ent["start"]] + tag + redacted[ent["end"]:]
        return redacted, entities

    def _chunk_sentences(self, text: str, max_tokens: int) -> List[str]:
        """Notebook-equivalent sentence chunking: no overlaps and no repetition."""
        sentences = sent_tokenize(text)
        chunks = []
        current_chunk = []
        current_tokens = 0

        for sentence in sentences:
            sentence_tokens = self.tokenizer.encode(sentence, add_special_tokens=False)
            sentence_token_len = len(sentence_tokens)

            if current_tokens + sentence_token_len <= max_tokens:
                current_chunk.append(sentence)
                current_tokens += sentence_token_len
            else:
                if current_chunk:
                    chunks.append(" ".join(current_chunk))
                current_chunk = [sentence]
                current_tokens = sentence_token_len

        if current_chunk:
            chunks.append(" ".join(current_chunk))

        return chunks

    def _chunk_sentences_with_metadata(self, text: str, max_tokens: int) -> List[Dict[str, Any]]:
        """
        Build sentence chunks with metadata to map chunk-local offsets back
        to original text offsets.
        """
        sentences = sent_tokenize(text)
        if not sentences:
            return []

        # Align each NLTK sentence with original-text offsets.
        aligned: List[Dict[str, Any]] = []
        cursor = 0
        for sentence in sentences:
            start = text.find(sentence, cursor)
            if start == -1:
                start = text.find(sentence)
            if start == -1:
                # Fallback keeps chunking stable even if alignment is imperfect.
                start = cursor
                end = min(len(text), start + len(sentence))
            else:
                end = start + len(sentence)
            cursor = end
            aligned.append({"text": sentence, "start": start, "end": end})

        chunks: List[Dict[str, Any]] = []
        current_sentences: List[Dict[str, Any]] = []
        current_tokens = 0

        for sentence_info in aligned:
            sentence_tokens = self.tokenizer.encode(
                sentence_info["text"],
                add_special_tokens=False,
            )
            sentence_token_len = len(sentence_tokens)

            if current_tokens + sentence_token_len <= max_tokens:
                current_sentences.append(sentence_info)
                current_tokens += sentence_token_len
            else:
                if current_sentences:
                    chunks.append(self._build_chunk_info(current_sentences))
                current_sentences = [sentence_info]
                current_tokens = sentence_token_len

        if current_sentences:
            chunks.append(self._build_chunk_info(current_sentences))

        return chunks

    def _build_chunk_info(self, sentences: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Compose notebook-style chunk text and local-to-global span mapping."""
        chunk_text = " ".join(sentence["text"] for sentence in sentences)
        segments: List[Dict[str, int]] = []
        local_cursor = 0
        for index, sentence in enumerate(sentences):
            local_start = local_cursor
            local_end = local_start + len(sentence["text"])
            segments.append(
                {
                    "local_start": local_start,
                    "local_end": local_end,
                    "global_start": sentence["start"],
                    "global_end": sentence["end"],
                }
            )
            local_cursor = local_end + (1 if index < len(sentences) - 1 else 0)
        return {"text": chunk_text, "segments": segments}

    def _map_chunk_entity_to_original(
        self, entity: Dict[str, Any], segments: List[Dict[str, int]]
    ) -> Optional[Tuple[int, int]]:
        """Map an entity detected in a chunk back to original text offsets."""
        ent_start = int(entity["start"])
        ent_end = int(entity["end"])
        for segment in segments:
            if ent_start >= segment["local_start"] and ent_end <= segment["local_end"]:
                rel_start = ent_start - segment["local_start"]
                rel_end = ent_end - segment["local_start"]
                return segment["global_start"] + rel_start, segment["global_start"] + rel_end
        return None
    
    def cleanup(self):
        """Cleanup resources."""
        self.model = None
        self.tokenizer = None
        self._initialized = False
        logger.info("GLiNER service cleaned up")
