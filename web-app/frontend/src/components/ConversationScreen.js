import React, { useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';
import ChatHeader from './ChatHeader';
import MessageList from './MessageList';
import ChatComposer from './ChatComposer';
import WarningModal from './WarningModal';
import './ConversationScreen.css';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';
const PII_DEBOUNCE_MS = 800;
const LLM_POST_PII_IDLE_MS = 2500;

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
  const typingTimeoutRef = useRef(null);
  const llmTimeoutRef = useRef(null);
  const livePipelineVersionRef = useRef(0);
  const riskRequestCounterRef = useRef(0);
  const sendInFlightRef = useRef(false);
  const assessAbortControllersRef = useRef({ pii: null, risk: null });

  const instructionSets = [
    {
      title: 'Scenario 1',
      content: [
        { type: 'paragraph', body: <>Please imagine that you are <strong>Kimberly</strong> in this conversation and continue the interaction as realistically as possible.</> },
        { type: 'paragraph', body: <>The suggested reply shows the intended goal of the response and may be copied directly to keep the intended goal of the response.</> },
        { type: 'bubble', label: 'Reference text', text: <>Here's my information: Kimberly Miller, June 15, 1986, 42 Maple Street, Springfield, IL.</>},
        { type: 'paragraph', body: <>Take on the role fully and interact naturally with the interface as you type.</> }
      ]
    },
    {
      title: 'Scenario 2',
      content: [
        { type: 'paragraph', body: <>Please imagine that you are <strong>Daniel</strong> in this conversation and continue the interaction as realistically as possible.</> },
        { type: 'paragraph', body: <>The suggested reply shows the intended goal of the response and may be copied directly to keep the intended goal of the response.</> },
        { type: 'bubble', label: 'Reference text', text: <>Certainly, Gerald! My full name is Daniel Thompson. I'm affiliated with Springfield University, and my research area is 'Neural Networks and Cognitive Computing.' My current project involves developing algorithms that improve decision-making processes for autonomous systems. I'll send my university email and phone number too. My email is daniel.thompson@springfield.edu, and my phone number is 555-526-7890. Is there anything else you need?</> },
        { type: 'paragraph', body: <>Take on the role fully and interact naturally with the interface as you type.</> }
      ]
    },
    {
      title: 'Scenario 3',
      content: [
        { type: 'paragraph', body: <>Please imagine that you are <strong>Matthew</strong> in this conversation and continue the interaction as realistically as possible.</> },
        { type: 'paragraph', body: <>The suggested reply shows the intended goal of the response and may be copied directly to keep the intended goal of the response.</> },
        { type: 'bubble', label: 'Reference text', text: <>I understand the need for verification. I'll send you my work email and that should suffice. It's matthew_1968@gmail.com</> },
        { type: 'paragraph', body: <>Take on the role fully and interact naturally with the interface as you type.</> }
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
    if (llmTimeoutRef.current) {
      clearTimeout(llmTimeoutRef.current);
      llmTimeoutRef.current = null;
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

  const queueLiveLlmStage = (pipelineVersion, textToUse) => {
    if (llmTimeoutRef.current) {
      clearTimeout(llmTimeoutRef.current);
      llmTimeoutRef.current = null;
    }
    llmTimeoutRef.current = setTimeout(() => {
      if (pipelineVersion !== livePipelineVersionRef.current) {
        return;
      }
      assessRisk(textToUse, { liveTyping: true });
    }, LLM_POST_PII_IDLE_MS);
  };

  const runLivePiiStage = async (pipelineVersion, textToUse) => {
    if (pipelineVersion !== livePipelineVersionRef.current) {
      return;
    }
    if (!textToUse.trim()) {
      return;
    }

    if (variant !== 'A') {
      setPiiSpans([]);
      queueLiveLlmStage(pipelineVersion, textToUse);
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

      queueLiveLlmStage(pipelineVersion, textToUse);
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
    const { openOnComplete = false, silent = false, liveTyping = false } = options;
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
      if (lastRawText && lastRawText.trim() === textToUse) {
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

        if (openOnComplete) {
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

    // Prevent a pending debounce-triggered assessment from racing and
    // overriding the explicit icon-click assessment request.
    clearLiveTimers();
    livePipelineVersionRef.current += 1;
    abortActiveAssessRequests();

    setIsWarningOpen(true);

    if (!warningState || lastAssessedText !== textToUse) {
      const assessment = await assessRisk(textToUse, { openOnComplete: true });
      if (assessment?.saferRewrite) {
        setLastShownRewrite(assessment.saferRewrite);
      }
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
      setDraftText(warningState.saferRewrite);
      setLastShownRewrite(warningState.saferRewrite);
    }
    setIsWarningOpen(false);
  };

  const handleContinueAnyway = () => {
    // Allow user to exit immediately even while analysis is still pending.
    // Bump request counter so stale in-flight responses are ignored.
    if (riskPending) {
      riskRequestCounterRef.current += 1;
      setRiskPending(false);
    }
    clearLiveTimers();
    livePipelineVersionRef.current += 1;
    abortActiveAssessRequests();
    setIsWarningOpen(false);
  };

  useEffect(() => () => {
    clearLiveTimers();
    abortActiveAssessRequests();
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
    <div className="conversation-screen">
      <ChatHeader contactName={contactName} scenario={conversation.scenario} />
      <MessageList messages={messages} conversationKey={conversationIndex} />
      <ChatComposer
        draftText={draftText}
        onTextChange={handleTyping}
        onSend={handleSend}
        variant={variant}
        piiSpans={piiSpans}
        onPiiClick={handleOpenWarning}
        isSending={isSending}
      />
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
