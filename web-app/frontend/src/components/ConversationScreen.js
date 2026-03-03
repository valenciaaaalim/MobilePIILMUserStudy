import React, { useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';
import ChatHeader from './ChatHeader';
import MessageList from './MessageList';
import ChatComposer from './ChatComposer';
import WarningModal from './WarningModal';
import './ConversationScreen.css';

const API_BASE_URL = process.env.REACT_APP_BACKEND_BASE_URL || 'http://localhost:8080';
const PII_DEBOUNCE_MS = 800;

function ConversationScreen({ conversation, sessionId, participantId, participantProlificId, variant, onComplete, conversationIndex }) {
  const navigate = useNavigate();
  const [messages, setMessages] = useState([]);
  const [draftText, setDraftText] = useState('');
  const [warningState, setWarningState] = useState(null);
  const [lastRiskAnalysis, setLastRiskAnalysis] = useState(null);
  const [riskPending, setRiskPending] = useState(false);
  const [isWarningOpen, setIsWarningOpen] = useState(false);
  const [currentMessageIndex, setCurrentMessageIndex] = useState(0);
  const [lastOfferedRewrite, setLastOfferedRewrite] = useState(null);
  const [lastShownRewrite, setLastShownRewrite] = useState(null);
  const [lastMaskedText, setLastMaskedText] = useState(null);
  const [lastRawText, setLastRawText] = useState(null);
  const [lastHasPii, setLastHasPii] = useState(false);
  const [lastAssessedText, setLastAssessedText] = useState('');
  const [isSending, setIsSending] = useState(false);
  const [maskedHistory, setMaskedHistory] = useState(null);
  const [piiSpans, setPiiSpans] = useState([]);
  const [isDrawerOpen, setIsDrawerOpen] = useState(false);
  const [composerHeight, setComposerHeight] = useState(60);
  const typingTimeoutRef = useRef(null);
  const livePipelineVersionRef = useRef(0);
  const riskRequestCounterRef = useRef(0);
  const sendInFlightRef = useRef(false);
  const assessAbortControllersRef = useRef({ pii: null, risk: null });
  const suppressAutoOpenRef = useRef(false);
  const composerContainerRef = useRef(null);

  const instructionSets = [
    {
      title: 'Scenario 1',
      content: [
        { type: 'paragraph', body: <>In the following conversation, you are responding as <strong>Kimberly</strong>. Continue the interaction as if it were real.</>},
        { type: 'bubble', label: 'Reference text', text: <>Here's my information: Kimberly Miller, June 15, 1986, 42 Maple Street, Springfield, IL.</>},
        { type: 'paragraph', body: <>The suggested reply above reflects the intended goal of the response and <strong>may be copied directly</strong>. If you make changes, <strong>please keep the same overall intent.</strong></>}
      ]
    },
    {
      title: 'Scenario 2',
      content: [
        { type: 'paragraph', body: <>In the following conversation, you are responding as <strong>Daniel</strong>. Continue the interaction as if it were real.</>},
        { type: 'bubble', label: 'Reference text', text: <>Certainly, Gerald! My full name is Daniel Thompson. I'm affiliated with Springfield University, and my research area is 'Neural Networks and Cognitive Computing.' My email is daniel.thompson@springfield.edu, and my phone number is 555-526-7890. Is there anything else you need?</>},
        { type: 'paragraph', body: <>The suggested reply above reflects the intended goal of the response and <strong>may be copied directly</strong>. If you make changes, <strong>please keep the same overall intent.</strong></>}
      ]
    },
    {
      title: 'Scenario 3',
      content: [
        { type: 'paragraph', body: <>In the following conversation, you are responding as <strong>Matthew</strong>. Continue the interaction as if it were real.</>},
        { type: 'bubble', label: 'Reference text', text: <>I understand the need for verification. I'll send you my work email and that should suffice. It's matthew_1968@gmail.com</> },
        { type: 'paragraph', body: <>The suggested reply above reflects the intended goal of the response and <strong>may be copied directly</strong>. If you make changes, <strong>please keep the same overall intent.</strong></>}      
      ]
    }
  ];
  const scenarioInstructions = instructionSets[conversationIndex];

  useEffect(() => {
    const allMessages = conversation.conversation || [];
    const initialMessages = allMessages.map((msg, idx) => ({
      ...msg,
      timestamp: new Date(Date.now() - (allMessages.length - idx) * 60000)
    }));

    setMessages(initialMessages);
    setCurrentMessageIndex(allMessages.length);
    setIsDrawerOpen(true);
    setWarningState(null);
    setLastRiskAnalysis(null);
    setRiskPending(false);
    setIsWarningOpen(false);
    setLastAssessedText('');
    setPiiSpans([]);

    const historyForMasking = initialMessages.map((m) => ({
      id: m.id,
      text: m.text,
      direction: m.direction,
      name: m.name || null,
      timestamp: m.timestamp || null
    }));

    let cancelled = false;
    const separator = '\n<<<MSG_SEPARATOR>>>\n';
    const serializedHistory = historyForMasking.map((m) => m.text || '').join(separator);
    axios.post(
      `${API_BASE_URL}/pii/detect`,
      { draft_text: serializedHistory },
      { timeout: 30000 }
    )
      .then((response) => {
        if (cancelled) return;
        const maskedText = response.data?.masked_text;
        if (!maskedText) {
          setMaskedHistory(historyForMasking);
          return;
        }
        const maskedParts = maskedText.split(separator);
        const rebuiltHistory = historyForMasking.map((m, idx) => ({
          ...m,
          text: maskedParts[idx] !== undefined ? maskedParts[idx] : m.text
        }));
        setMaskedHistory(rebuiltHistory);
      })
      .catch(() => {
        if (!cancelled) {
          setMaskedHistory(historyForMasking);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [conversation]);

  const handleToggleDrawer = () => {
    setIsDrawerOpen((open) => !open);
  };

  const handleCloseDrawer = () => {
    setIsDrawerOpen(false);
  };

  const clearLiveTimers = () => {
    if (typingTimeoutRef.current) {
      clearTimeout(typingTimeoutRef.current);
      typingTimeoutRef.current = null;
    }
  };

  const abortActiveAssessRequests = () => {
    const { pii, risk } = assessAbortControllersRef.current;
    if (pii) {
      pii.abort();
    }
    if (risk) {
      risk.abort();
    }
    assessAbortControllersRef.current = { pii: null, risk: null };
  };

  const isCanceledRequest = (error) => (
    error?.code === 'ERR_CANCELED'
    || error?.name === 'CanceledError'
    || error?.name === 'AbortError'
  );

  const runLivePiiStage = async (pipelineVersion, textToUse) => {
    if (pipelineVersion !== livePipelineVersionRef.current) {
      return;
    }
    if (!textToUse.trim()) {
      return;
    }

    if (variant !== 'A') {
      setPiiSpans([]);
      return;
    }

    let piiController = null;
    try {
      piiController = new AbortController();
      assessAbortControllersRef.current.pii = piiController;
      const piiResponse = await axios.post(
        `${API_BASE_URL}/pii/detect`,
        { draft_text: textToUse },
        { timeout: 30000, signal: piiController.signal }
      );
      if (pipelineVersion !== livePipelineVersionRef.current) {
        return;
      }

      const spans = piiResponse.data?.pii_spans || [];
      const masked = piiResponse.data?.masked_text || null;
      const hasPii = spans.length > 0;

      setPiiSpans(spans);
      setLastRawText(textToUse);
      setLastMaskedText(masked);
      setLastHasPii(hasPii);

      if (!hasPii) {
        setWarningState(null);
        setLastRiskAnalysis(null);
        setLastOfferedRewrite(null);
        setLastShownRewrite(null);
        setLastAssessedText(textToUse);
        setRiskPending(false);
        setIsWarningOpen(false);
        return;
      }
    } catch (error) {
      if (isCanceledRequest(error)) {
        return;
      }
      if (pipelineVersion !== livePipelineVersionRef.current) {
        return;
      }
      console.error('[RISK] PII detection failed in live stage:', error);
      setPiiSpans([]);
      setLastRawText(textToUse);
      setLastMaskedText(null);
      setLastHasPii(false);
      setWarningState(null);
      setLastRiskAnalysis(null);
      setLastOfferedRewrite(null);
      setLastShownRewrite(null);
      setLastAssessedText(textToUse);
      setRiskPending(false);
      setIsWarningOpen(false);
    } finally {
      if (piiController && assessAbortControllersRef.current.pii === piiController) {
        assessAbortControllersRef.current.pii = null;
      }
    }
  };

  const handleTyping = (text) => {
    setDraftText(text);

    // New text input should immediately invalidate/abort prior analysis.
    clearLiveTimers();
    livePipelineVersionRef.current += 1;
    riskRequestCounterRef.current += 1;
    abortActiveAssessRequests();
    setRiskPending(false);

    if (text.trim()) {
      const pipelineVersion = livePipelineVersionRef.current;
      const textToUse = text.trim();
      typingTimeoutRef.current = setTimeout(() => {
        runLivePiiStage(pipelineVersion, textToUse);
      }, PII_DEBOUNCE_MS);
    } else {
      setWarningState(null);
      setLastRiskAnalysis(null);
      setRiskPending(false);
      setIsWarningOpen(false);
      setLastOfferedRewrite(null);
      setLastShownRewrite(null);
      setLastMaskedText(null);
      setLastRawText(null);
      setLastHasPii(false);
      setLastAssessedText('');
      setPiiSpans([]);
    }
  };

  const toSingleLineReasoning = (value) => {
    const cleaned = (value || '').replace(/\s+/g, ' ').trim();
    if (!cleaned) return '';
    const firstSentence = cleaned.split(/(?<=[.!?])\s+/)[0];
    return firstSentence.length > 180 ? `${firstSentence.slice(0, 177)}...` : firstSentence;
  };

  const assessRisk = async (text, options = {}) => {
    const textToUse = text.trim();
    const {
      openOnComplete = false,
      silent = false,
      liveTyping = false,
      forcePiiRefresh = false
    } = options;
    if (!textToUse) return null;

    const requestId = ++riskRequestCounterRef.current;
    if (!silent) {
      setRiskPending(true);
      if (openOnComplete) {
        setIsWarningOpen(true);
      }
    }
    // A new assessment should always cancel any prior in-flight analysis calls.
    abortActiveAssessRequests();

    let maskedToUse = null;
    let hasPii = variant !== 'A';
    let piiController = null;
    let riskController = null;
    if (variant !== 'A') {
      setPiiSpans([]);
    }

    if (variant === 'A') {
      if (!forcePiiRefresh && lastRawText && lastRawText.trim() === textToUse) {
        maskedToUse = lastMaskedText;
        hasPii = lastHasPii;
      } else {
        try {
          piiController = new AbortController();
          assessAbortControllersRef.current.pii = piiController;
          const piiResponse = await axios.post(
            `${API_BASE_URL}/pii/detect`,
            { draft_text: textToUse },
            { timeout: 30000, signal: piiController.signal }
          );
          if (requestId !== riskRequestCounterRef.current) {
            return null;
          }
          const spans = piiResponse.data?.pii_spans || [];
          maskedToUse = piiResponse.data?.masked_text || null;
          hasPii = spans.length > 0;
          setPiiSpans(spans);
          setLastRawText(textToUse);
          setLastMaskedText(maskedToUse);
          setLastHasPii(hasPii);
        } catch (error) {
          if (isCanceledRequest(error)) {
            return null;
          }
          console.error('[RISK] PII detection failed for risk assessment:', error);
          hasPii = false;
          maskedToUse = null;
          setPiiSpans([]);
        }
      }

      if (!hasPii) {
        setPiiSpans([]);
        if (!silent) {
          setWarningState(null);
          setLastRiskAnalysis(null);
          setLastOfferedRewrite(null);
          setLastShownRewrite(null);
          setLastAssessedText(textToUse);
          setRiskPending(false);
          if (!openOnComplete) {
            setIsWarningOpen(false);
          }
        }
        return null;
      }
    }

    try {
      const conversationHistory = messages.map((m) => ({
        id: m.id,
        text: m.text,
        direction: m.direction,
        name: m.name || null,
        timestamp: m.timestamp || null
      }));

      riskController = new AbortController();
      assessAbortControllersRef.current.risk = riskController;
      const response = await axios.post(`${API_BASE_URL}/api/risk/assess`, {
        draft_text: textToUse,
        masked_text: maskedToUse,
        masked_history: maskedHistory || conversationHistory,
        conversation_history: conversationHistory,
        session_id: conversationIndex + 1,
        participant_prolific_id: participantProlificId || null,
        live_typing: Boolean(liveTyping)
      }, { signal: riskController.signal });

      if (requestId !== riskRequestCounterRef.current) {
        return null;
      }

      const rewrite = response.data.safer_rewrite || '';
      const assessment = {
        riskLevel: response.data.risk_level,
        saferRewrite: rewrite,
        primaryRiskFactors: response.data.primary_risk_factors || [],
        reasoning: toSingleLineReasoning(
          response.data.reasoning || response.data.output_2?.reasoning || ''
        ),
        originalInput: response.data.output_2?.original_user_message || maskedToUse || textToUse,
        output1: response.data.output_1 || {},
        output2: response.data.output_2 || {}
      };

      if (!silent) {
        setWarningState(assessment);
        setLastRiskAnalysis(assessment);
        setLastAssessedText(textToUse);

        if (variant === 'A' && rewrite && rewrite.trim() && rewrite.trim() !== textToUse) {
          setLastOfferedRewrite(rewrite);
        }

        if (openOnComplete && !suppressAutoOpenRef.current) {
          setIsWarningOpen(true);
          if (rewrite && rewrite.trim()) {
            setLastShownRewrite(rewrite);
          }
        }
      }

      return assessment;
    } catch (error) {
      if (isCanceledRequest(error)) {
        return null;
      }
      if (!silent && requestId === riskRequestCounterRef.current) {
        console.error('Error assessing risk:', error);
        setWarningState(null);
        setLastRiskAnalysis(null);
        if (!openOnComplete) {
          setIsWarningOpen(false);
        }
      }
      return null;
    } finally {
      if (piiController && assessAbortControllersRef.current.pii === piiController) {
        assessAbortControllersRef.current.pii = null;
      }
      if (riskController && assessAbortControllersRef.current.risk === riskController) {
        assessAbortControllersRef.current.risk = null;
      }
      if (!silent && requestId === riskRequestCounterRef.current) {
        setRiskPending(false);
      }
    }
  };

  const handleOpenWarning = async () => {
    const textToUse = draftText.trim();
    if (!textToUse) {
      return;
    }
    setIsDrawerOpen(false);
    suppressAutoOpenRef.current = false;

    // Stop pending typing debounce before explicit analysis click.
    clearLiveTimers();
    livePipelineVersionRef.current += 1;

    setIsWarningOpen(true);

    if (riskPending) {
      return;
    }

    if (warningState && lastAssessedText === textToUse) {
      return;
    }

    const assessment = await assessRisk(textToUse, {
      openOnComplete: true,
      forcePiiRefresh: true
    });
    if (assessment?.saferRewrite) {
      setLastShownRewrite(assessment.saferRewrite);
    }
  };

  const handleSend = async () => {
    if (!draftText.trim() || sendInFlightRef.current) return;

    sendInFlightRef.current = true;
    setIsSending(true);
    clearLiveTimers();
    livePipelineVersionRef.current += 1;
    riskRequestCounterRef.current += 1;
    abortActiveAssessRequests();

    const finalText = draftText.trim();
    let analysis = warningState || lastRiskAnalysis;

    const newMessage = {
      id: `sent-${Date.now()}`,
      text: finalText,
      direction: 'SENT',
      timestamp: new Date()
    };

    // Optimistic UI update keeps send interaction responsive.
    setMessages((prev) => [...prev, newMessage]);
    setDraftText('');
    setWarningState(null);
    setLastRiskAnalysis(null);
    setRiskPending(false);
    setLastOfferedRewrite(null);
    setLastShownRewrite(null);
    setIsWarningOpen(false);
    setLastAssessedText('');
    setPiiSpans([]);
    setCurrentMessageIndex((prev) => prev + 1);

    if (variant === 'A' && (!analysis || lastAssessedText !== finalText)) {
      const refreshed = await assessRisk(finalText, { openOnComplete: false, silent: true });
      if (refreshed) {
        analysis = refreshed;
      }
    }

    const originalInput = analysis?.originalInput || finalText;
    const finalMaskedText = (lastRawText && lastRawText.trim() === finalText)
      ? lastMaskedText
      : null;
    const finalRewriteText = analysis?.saferRewrite || warningState?.saferRewrite || lastShownRewrite || lastOfferedRewrite;

    try {
      const output1 = analysis?.output1 || {};

      const messagePayload = {
        participant_id: participantProlificId,
        conversation_index: conversationIndex,
        final_message: finalText,
        variant
      };

      if (variant === 'A') {
        messagePayload.original_input = originalInput;
        messagePayload.final_masked_text = finalMaskedText;
        if (finalRewriteText) {
          messagePayload.final_rewrite_text = finalRewriteText;
        }
      }

      if (analysis) {
        messagePayload.risk_level = analysis.riskLevel || null;
        messagePayload.primary_risk_factors = analysis.primaryRiskFactors || [];
        messagePayload.reasoning = analysis.reasoning || '';

        messagePayload.linkability_risk_level = output1.linkability_risk?.level || null;
        messagePayload.linkability_risk_explanation = output1.linkability_risk?.explanation || null;
        messagePayload.authentication_baiting_level = output1.authentication_baiting?.level || null;
        messagePayload.authentication_baiting_explanation = output1.authentication_baiting?.explanation || null;
        messagePayload.contextual_alignment_level = output1.contextual_alignment?.level || null;
        messagePayload.contextual_alignment_explanation = output1.contextual_alignment?.explanation || null;
        messagePayload.platform_trust_obligation_level = output1.platform_trust_obligation?.level || null;
        messagePayload.platform_trust_obligation_explanation = output1.platform_trust_obligation?.explanation || null;
        messagePayload.psychological_pressure_level = output1.psychological_pressure?.level || null;
        messagePayload.psychological_pressure_explanation = output1.psychological_pressure?.explanation || null;
      }

      await axios.post(`${API_BASE_URL}/api/participants/message`, messagePayload);
    } catch (error) {
      console.error('[Send] error capturing user input', error);
    } finally {
      sendInFlightRef.current = false;
      setIsSending(false);
    }

    const allMessages = conversation.conversation || [];
    const userTypedMessages = messages.filter((m) => m.id && m.id.startsWith('sent-')).length;

    if (userTypedMessages > 0 || currentMessageIndex >= allMessages.length) {
      setTimeout(() => {
        onComplete();
        navigate(`/survey/mid?index=${conversationIndex}`);
      }, 2000);
    }
  };

  const handleAcceptRewrite = () => {
    if (warningState && warningState.saferRewrite) {
      const rewriteText = warningState.saferRewrite;
      // Clear stale underline immediately, then route through the normal
      // typing pipeline so GLiNER re-runs on the accepted rewrite.
      setPiiSpans([]);
      handleTyping(rewriteText);
      setLastShownRewrite(rewriteText);
    }
    setIsWarningOpen(false);
  };

  const handleContinueAnyway = () => {
    // Keep in-flight explicit analysis alive so it can be reused later
    // when the user has not changed the draft.
    if (riskPending) {
      suppressAutoOpenRef.current = true;
    }
    clearLiveTimers();
    setIsWarningOpen(false);
  };

  useEffect(() => () => {
    clearLiveTimers();
    abortActiveAssessRequests();
  }, []);

  useEffect(() => {
    const composerNode = composerContainerRef.current;
    if (!composerNode) {
      return undefined;
    }

    const updateComposerHeight = () => {
      const measuredHeight = Math.ceil(composerNode.getBoundingClientRect().height);
      setComposerHeight(measuredHeight || 60);
    };

    updateComposerHeight();

    if (typeof ResizeObserver === 'undefined') {
      window.addEventListener('resize', updateComposerHeight);
      return () => {
        window.removeEventListener('resize', updateComposerHeight);
      };
    }

    const resizeObserver = new ResizeObserver(updateComposerHeight);
    resizeObserver.observe(composerNode);

    return () => {
      resizeObserver.disconnect();
    };
  }, []);

  const getContactName = () => {
    const convData = conversation.conversation || [];
    const firstReceived = convData.find((m) => m.direction === 'RECEIVED');
    const fullName = firstReceived?.name || 'Contact';
    const nameOnly = fullName.split(' - ')[0].split(' | ')[0].trim();
    return nameOnly;
  };

  const contactName = getContactName();

  return (
    <div
      className="conversation-screen"
      style={{ '--composer-height': `${composerHeight}px` }}
    >
      <ChatHeader contactName={contactName} scenario={conversation.scenario} />
      <MessageList messages={messages} conversationKey={conversationIndex} />
      <div ref={composerContainerRef} className="conversation-composer-layer">
        <ChatComposer
          draftText={draftText}
          onTextChange={handleTyping}
          onSend={handleSend}
          variant={variant}
          piiSpans={piiSpans}
          onPiiClick={handleOpenWarning}
          isSending={isSending}
        />
      </div>
      {isWarningOpen && (riskPending || warningState) && (
        <WarningModal
          warningState={warningState}
          riskPending={riskPending}
          onAcceptRewrite={handleAcceptRewrite}
          onContinueAnyway={handleContinueAnyway}
        />
      )}

      {isDrawerOpen && <div className="drawer-overlay" onClick={handleCloseDrawer} />}

      <div className={`instructions-drawer ${isDrawerOpen ? 'open' : ''}`}>
        <button
          type="button"
          className="drawer-tab"
          onClick={handleToggleDrawer}
        >
          {isDrawerOpen ? 'Close' : 'Instructions'}
        </button>
        <div className="drawer-panel">
          <div className="drawer-header">
            <h2>{scenarioInstructions?.title || ''}</h2>
            <button
              type="button"
              className="drawer-close-button"
              onClick={handleCloseDrawer}
            >
              Close
            </button>
          </div>
          <div className="drawer-content">
            {(scenarioInstructions?.content || []).map((item, idx) => {
              if (item.type === 'bubble') {
                if (!item.text) return null;
                return (
                  <div key={`${conversationIndex}-instruction-bubble-${idx}`} className="instruction-bubble">
                    <div className="instruction-bubble__label">{item.label || 'Reference text'}</div>
                    <div className="instruction-bubble__message">
                      {item.text}
                    </div>
                  </div>

                );
              }
              return (
                <p key={`${conversationIndex}-instruction-paragraph-${idx}`}>{item.body}</p>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}

export default ConversationScreen;
