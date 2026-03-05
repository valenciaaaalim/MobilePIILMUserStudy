import React, { useEffect, useRef } from 'react';
import './ChatComposer.css';

const INPUT_LINE_HEIGHT_PX = 20;
const INPUT_VERTICAL_PADDING_PX = 16;
const INPUT_MIN_HEIGHT_PX = INPUT_LINE_HEIGHT_PX + INPUT_VERTICAL_PADDING_PX;
const INPUT_MAX_LINES = 5;
const INPUT_MAX_HEIGHT_PX = (INPUT_LINE_HEIGHT_PX * INPUT_MAX_LINES) + INPUT_VERTICAL_PADDING_PX;

function ChatComposer({
  draftText,
  onTextChange,
  onSend,
  variant,
  piiSpans = [],
  onPiiClick,
  isSending,
  sendDisabled = false,
  inputDisabled = false
}) {
  const textareaRef = useRef(null);
  const overlayRef = useRef(null);
  const piiBubbleRef = useRef(null);

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
      if (isSending || sendDisabled || e.repeat) {
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

    // Normalize and merge spans so adjacent/overlapping sensitive words render
    // as one continuous highlight/underline block.
    const normalizedSpans = piiSpans
      .map((span) => ({
        start: Math.max(0, Math.min(draftText.length, Number(span.start))),
        end: Math.max(0, Math.min(draftText.length, Number(span.end))),
      }))
      .filter((span) => span.end > span.start)
      .sort((a, b) => a.start - b.start);

    const mergedSpans = [];
    normalizedSpans.forEach((span) => {
      const last = mergedSpans[mergedSpans.length - 1];
      if (!last) {
        mergedSpans.push({ ...span });
        return;
      }

      const gapText = draftText.slice(last.end, span.start);
      const canMerge = span.start <= last.end || /^\s*$/.test(gapText);
      if (canMerge) {
        last.end = Math.max(last.end, span.end);
      } else {
        mergedSpans.push({ ...span });
      }
    });

    const parts = [];
    let lastIndex = 0;

    mergedSpans.forEach((span, idx) => {
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
            disabled={isSending || inputDisabled}
          />
        </div>
        <button
          className="send-button"
          onClick={onSend}
          disabled={isSending || sendDisabled || !draftText.trim()}
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
