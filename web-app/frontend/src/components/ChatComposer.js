import React, { useState, useEffect, useRef } from 'react';
import axios from 'axios';
import './ChatComposer.css';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';
const INPUT_LINE_HEIGHT_PX = 20;
const INPUT_VERTICAL_PADDING_PX = 16;
const INPUT_MIN_HEIGHT_PX = INPUT_LINE_HEIGHT_PX + INPUT_VERTICAL_PADDING_PX;
const INPUT_MAX_LINES = 5;
const INPUT_MAX_HEIGHT_PX = (INPUT_LINE_HEIGHT_PX * INPUT_MAX_LINES) + INPUT_VERTICAL_PADDING_PX;

function ChatComposer({ draftText, onTextChange, onSend, variant, onPiiDetected, onPiiClick, isSending }) {
  const [piiSpans, setPiiSpans] = useState([]);
  const debounceTimeoutRef = useRef(null);
  const requestCounterRef = useRef(0);
  const textareaRef = useRef(null);
  const overlayRef = useRef(null);
  const piiBubbleRef = useRef(null);
  const detectAbortRef = useRef(null);

  const triggerPiiBubblePulse = () => {
    const bubble = piiBubbleRef.current;
    if (!bubble) {
      return;
    }
    bubble.classList.remove('pii-alert-bubble--pulse');
    // Force reflow so the animation restarts on repeated clicks.
    void bubble.offsetWidth;
    bubble.classList.add('pii-alert-bubble--pulse');
  };

  useEffect(() => {
    if (!textareaRef.current) {
      return;
    }

    const textarea = textareaRef.current;
    textarea.style.height = 'auto';

    const nextHeight = Math.min(
      Math.max(textarea.scrollHeight, INPUT_MIN_HEIGHT_PX),
      INPUT_MAX_HEIGHT_PX
    );
    textarea.style.height = `${nextHeight}px`;
    textarea.style.overflowY = textarea.scrollHeight > INPUT_MAX_HEIGHT_PX ? 'auto' : 'hidden';

    if (overlayRef.current) {
      overlayRef.current.style.height = `${nextHeight}px`;
      overlayRef.current.style.overflowY = textarea.style.overflowY;
      overlayRef.current.scrollTop = textarea.scrollTop;
      overlayRef.current.scrollLeft = textarea.scrollLeft;
    }
  }, [draftText, variant]);

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey && !e.nativeEvent.isComposing) {
      e.preventDefault();
      if (isSending || e.repeat) {
        return;
      }
      onSend();
    }
  };

  const handleInputClick = () => {
    if (variant !== 'A' || piiSpans.length === 0 || !textareaRef.current) {
      return;
    }
    const caretIndex = textareaRef.current.selectionStart;
    if (caretIndex === null || caretIndex === undefined) {
      return;
    }
    const clickedPii = piiSpans.some((span) => caretIndex >= span.start && caretIndex <= span.end);
    if (clickedPii) {
      triggerPiiBubblePulse();
    }
  };

  // PII detection for Group A only
  useEffect(() => {
    // Only enable PII detection for Group A
    if (variant !== 'A') {
      if (detectAbortRef.current) {
        detectAbortRef.current.abort();
        detectAbortRef.current = null;
      }
      setPiiSpans([]);
      return;
    }

    // Clear previous timeout
    if (debounceTimeoutRef.current) {
      clearTimeout(debounceTimeoutRef.current);
    }

    // If text is empty, clear PII spans
    if (!draftText.trim()) {
      if (detectAbortRef.current) {
        detectAbortRef.current.abort();
        detectAbortRef.current = null;
      }
      setPiiSpans([]);
      return;
    }

    // Debounce PII detection (800ms as per requirements)
    debounceTimeoutRef.current = setTimeout(() => {
      const currentRequest = ++requestCounterRef.current;
      console.log('[PII] debounce fired', { length: draftText.length, request: currentRequest });

      if (detectAbortRef.current) {
        detectAbortRef.current.abort();
      }
      const controller = new AbortController();
      detectAbortRef.current = controller;
      
      axios.post(
        `${API_BASE_URL}/pii/detect`,
        { draft_text: draftText },
        { timeout: 30000, signal: controller.signal }
      )
      .then(response => {
        // Ignore stale responses
        if (currentRequest === requestCounterRef.current) {
          console.log('[PII] detect success', {
            request: currentRequest,
            spans: response.data?.pii_spans?.length || 0
          });
          const spans = response.data.pii_spans || [];
          const masked = response.data.masked_text || '';
          setPiiSpans(spans);
          // Notify parent component of PII detection results
          if (onPiiDetected) {
            onPiiDetected({
              piiSpans: spans,
              maskedText: masked,
              hasPii: spans.length > 0,
              sourceText: draftText
            });
          }
        }
      })
      .catch(error => {
        if (error?.code === 'ERR_CANCELED' || error?.name === 'CanceledError' || error?.name === 'AbortError') {
          return;
        }
        console.error('[PII] detect error', error);
        // Ignore stale responses
        if (currentRequest === requestCounterRef.current) {
          setPiiSpans([]);
        }
      });
    }, 800); // 1500ms debounce as per requirements

    return () => {
      if (debounceTimeoutRef.current) {
        clearTimeout(debounceTimeoutRef.current);
      }
      if (detectAbortRef.current) {
        detectAbortRef.current.abort();
        detectAbortRef.current = null;
      }
    };
  }, [draftText, variant]);

  // Sync scroll between textarea and overlay
  const handleScroll = () => {
    if (overlayRef.current && textareaRef.current) {
      overlayRef.current.scrollTop = textareaRef.current.scrollTop;
      overlayRef.current.scrollLeft = textareaRef.current.scrollLeft;
    }
  };

  // Render text with PII underlines for overlay
  const renderOverlayText = () => {
    if (piiSpans.length === 0) {
      return draftText;
    }

    // Sort spans by start position
    const sortedSpans = [...piiSpans].sort((a, b) => a.start - b.start);
    const parts = [];
    let lastIndex = 0;

    sortedSpans.forEach((span, idx) => {
      // Add text before span
      if (span.start > lastIndex) {
        parts.push({
          text: draftText.substring(lastIndex, span.start),
          isPii: false,
          key: `before-${idx}`
        });
      }
      // Add PII span
      parts.push({
        text: draftText.substring(span.start, span.end),
        isPii: true,
        key: `pii-${idx}`
      });
      lastIndex = span.end;
    });

    // Add remaining text
    if (lastIndex < draftText.length) {
      parts.push({
        text: draftText.substring(lastIndex),
        isPii: false,
        key: 'after'
      });
    }

    return parts.map(part => (
      <span key={part.key} className={part.isPii ? 'pii-underline' : ''}>
        {part.text}
      </span>
    ));
  };

  return (
    <div className="chat-composer">
      <div className="composer-content">
        <div className="textarea-wrapper">
          {variant === 'A' && piiSpans.length > 0 && (
            <button
              ref={piiBubbleRef}
              type="button"
              className="pii-alert-bubble"
              onClick={onPiiClick}
              onAnimationEnd={(e) => {
                e.currentTarget.classList.remove('pii-alert-bubble--pulse');
              }}
            >
              !
            </button>
          )}
          {/* PII Underline Overlay (Group A only) */}
          {variant === 'A' && (
            <div
              ref={overlayRef}
              className="pii-overlay"
            >
              {renderOverlayText()}
            </div>
          )}
          <textarea
            ref={textareaRef}
            className="message-input"
            placeholder="Type a message"
            value={draftText}
            onChange={(e) => onTextChange(e.target.value)}
            onKeyDown={handleKeyDown}
            onScroll={handleScroll}
            onClick={handleInputClick}
            rows={1}
            disabled={isSending}
          />
        </div>
        <button
          className="send-button"
          onClick={onSend}
          disabled={isSending || !draftText.trim()}
        >
          <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
            <path
              d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z"
              fill="currentColor"
            />
          </svg>
        </button>
      </div>
    </div>
  );
}

export default ChatComposer;
