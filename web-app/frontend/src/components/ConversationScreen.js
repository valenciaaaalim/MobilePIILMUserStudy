import React, { useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';
import ChatHeader from './ChatHeader';
import MessageList from './MessageList';
import ChatComposer from './ChatComposer';
import WarningModal from './WarningModal';
import './ConversationScreen.css';
import { getRedirectPathFrom409 } from '../utils/apiErrors';

const API_BASE_URL = process.env.REACT_APP_BACKEND_BASE_URL || 'http://localhost:8080';
const DEFAULT_PII_DEBOUNCE_MS = 400;

function ConversationScreen({ conversation, participantId, participantProlificId, variant, onComplete, conversationIndex }) {
  const navigate = useNavigate();
  const [messages, setMessages] = useState([]);
  const [draftText, setDraftText] = useState('');
  const [warningState, setWarningState] = useState(null);
  const [lastRiskAnalysis, setLastRiskAnalysis] = useState(null);
  const [riskPending, setRiskPending] = useState(false);
  const [isWarningOpen, setIsWarningOpen] = useState(false);
  const [riskError, setRiskError] = useState(false);
  const [capReached, setCapReached] = useState(false);
  const [currentMessageIndex, setCurrentMessageIndex] = useState(0);
  const [, setLastOfferedRewrite] = useState(null);
  const [, setLastShownRewrite] = useState(null);
  const [lastMaskedText, setLastMaskedText] = useState(null);
  const [lastRawText, setLastRawText] = useState(null);
  const [lastHasPii, setLastHasPii] = useState(false);
  const [lastAssessedText, setLastAssessedText] = useState('');
  const [isSending, setIsSending] = useState(false);
  const [maskedHistory, setMaskedHistory] = useState(null);
  const [piiSpans, setPiiSpans] = useState([]);
  const [isDrawerOpen, setIsDrawerOpen] = useState(false);
  const [composerHeight, setComposerHeight] = useState(60);
  const [isSubmitTransitioning, setIsSubmitTransitioning] = useState(false);
  const [submitLoadingText, setSubmitLoadingText] = useState('Loading');
  const [piiDebounceMs, setPiiDebounceMs] = useState(DEFAULT_PII_DEBOUNCE_MS);
  const typingTimeoutRef = useRef(null);
  const livePipelineVersionRef = useRef(0);
  const riskRequestCounterRef = useRef(0);
  const sendInFlightRef = useRef(false);
  const assessAbortControllersRef = useRef({ pii: null, risk: null });
  const submitLoadingDelayRef = useRef(null);
  const suppressAutoOpenRef = useRef(false);
  const composerContainerRef = useRef(null);
  const pendingAbortMarkerRef = useRef(false);
  const pendingRiskOutputIdRef = useRef(null);
  const activeAlertInteractionRef = useRef(null);

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
    let cancelled = false;
    axios.get(`${API_BASE_URL}/pii/config`, { timeout: 5000 })
      .then((response) => {
        if (cancelled) return;
        const rawMs = Number(response?.data?.gliner_debounce_ms);
        if (Number.isFinite(rawMs) && rawMs >= 0) {
          setPiiDebounceMs(Math.round(rawMs));
        }
      })
      .catch(() => {
        // Keep default debounce if config endpoint is unavailable.
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    const allMessages = conversation.conversation || [];
    const initialMessages = allMessages.map((msg, idx) => ({
      ...msg,
      timestamp: new Date(Date.now() - (allMessages.length - idx) * 60000)
    }));

    setMessages(initialMessages);
    setCurrentMessageIndex(allMessages.length);
    // Scenario 1 starts with instructions open; scenarios 2+ start closed.
    setIsDrawerOpen(conversationIndex === 0);
    setWarningState(null);
    setLastRiskAnalysis(null);
    setRiskPending(false);
    setIsWarningOpen(false);
    setLastAssessedText('');
    setPiiSpans([]);
    setRiskError(false);
    setCapReached(false);
    activeAlertInteractionRef.current = null;

    // Restore draft from sessionStorage if the user refreshed mid-typing
    try {
      const savedDraft = sessionStorage.getItem(`draft_conv_${conversationIndex}`);
      if (savedDraft) setDraftText(savedDraft);
    } catch (_) { /* ignore */ }

    const historyForMasking = initialMessages.map((m) => ({
      id: m.id,
      text: m.text,
      direction: m.direction,
      name: m.name || null,
      timestamp: m.timestamp || null
    }));

    let cancelled = false;
    if (variant !== 'A') {
      setMaskedHistory(historyForMasking);
      return () => {
        cancelled = true;
      };
    }

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
  }, [conversation, conversationIndex, variant]);

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

  const navigateOnStepConflict = (error) => {
    const redirectPath = getRedirectPathFrom409(error);
    if (!redirectPath) {
      return false;
    }
    navigate(redirectPath, { replace: true });
    return true;
  };

  const startAlertInteraction = async () => {
    if (variant !== 'A' || !participantId) {
      return null;
    }
    try {
      const response = await axios.post(
        `${API_BASE_URL}/api/participants/${participantId}/alert-interactions/start`,
        { scenario_number: conversationIndex + 1 }
      );
      activeAlertInteractionRef.current = response.data;
      return response.data;
    } catch (error) {
      if (!navigateOnStepConflict(error)) {
        console.error('[ALERT] Failed to start interaction row:', error);
      }
      activeAlertInteractionRef.current = null;
      return null;
    }
  };

  const completeAlertInteraction = async (scenarioResponseId, assessment) => {
    if (variant !== 'A' || !participantId || !scenarioResponseId || !assessment) {
      return false;
    }

    const output1 = assessment.output1 || {};
    try {
      await axios.post(
        `${API_BASE_URL}/api/participants/${participantId}/alert-interactions/${scenarioResponseId}/complete`,
        {
          scenario_number: conversationIndex + 1,
          original_input: assessment.analysisInput || assessment.originalInput || null,
          masked_text: assessment.maskedInput || null,
          output_id: assessment.outputId || null,
          input_tokens: assessment.inputTokens ?? null,
          total_tokens: assessment.totalTokens ?? null,
          model: assessment.model || null,
          risk_level: assessment.riskLevel || null,
          reasoning: assessment.reasoning || null,
          suggested_rewrite: assessment.saferRewrite || null,
          primary_risk_factors: assessment.primaryRiskFactors || [],
          linkability_risk_level: output1.linkability_risk?.level || null,
          linkability_risk_explanation: output1.linkability_risk?.explanation || null,
          authentication_baiting_level: output1.authentication_baiting?.level || null,
          authentication_baiting_explanation: output1.authentication_baiting?.explanation || null,
          contextual_alignment_level: output1.contextual_alignment?.level || null,
          contextual_alignment_explanation: output1.contextual_alignment?.explanation || null,
          platform_trust_obligation_level: output1.platform_trust_obligation?.level || null,
          platform_trust_obligation_explanation: output1.platform_trust_obligation?.explanation || null,
          psychological_pressure_level: output1.psychological_pressure?.level || null,
          psychological_pressure_explanation: output1.psychological_pressure?.explanation || null
        }
      );
      return true;
    } catch (error) {
      if (!navigateOnStepConflict(error)) {
        console.error('[ALERT] Failed to complete interaction row:', error);
      }
      activeAlertInteractionRef.current = null;
      return false;
    }
  };

  const recordAlertDecision = async (acceptedRewrite) => {
    const scenarioResponseId = activeAlertInteractionRef.current?.id;
    if (variant !== 'A' || !participantId || !scenarioResponseId) {
      return;
    }
    try {
      await axios.post(
        `${API_BASE_URL}/api/participants/${participantId}/alert-interactions/${scenarioResponseId}/decision`,
        { accepted_rewrite: acceptedRewrite }
      );
    } catch (error) {
      if (!navigateOnStepConflict(error)) {
        console.error('[ALERT] Failed to persist interaction decision:', error);
      }
    }
  };

  const createOutputId = () => {
    if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
      return crypto.randomUUID();
    }
    return `output-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
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

  const draftStorageKey = `draft_conv_${conversationIndex}`;

  const handleTyping = (text, options = {}) => {
    const { preserveDecision = false } = options;
    setDraftText(text);
    try { sessionStorage.setItem(draftStorageKey, text); } catch (_) { /* quota */ }
    pendingAbortMarkerRef.current = false;
    void preserveDecision;

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
      }, piiDebounceMs);
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
    const outputId = createOutputId();
    pendingRiskOutputIdRef.current = outputId;
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
        output_id: outputId
      }, { signal: riskController.signal });

      if (requestId !== riskRequestCounterRef.current) {
        return null;
      }

      const rewrite = response.data.safer_rewrite || '';
      const isCapped = !!response.data.cap_reached;
      const capNoCache = !!response.data.cap_no_cache;

      if (isCapped && capNoCache) {
        // Capped with no prior result — show error state in modal
        if (!silent && requestId === riskRequestCounterRef.current) {
          setCapReached(true);
          setRiskError(true);
          setRiskPending(false);
          setWarningState(null);
        }
        return null;
      }

      if (isCapped) {
        setCapReached(true);
      }

      const assessment = {
        riskLevel: response.data.risk_level,
        saferRewrite: rewrite,
        primaryRiskFactors: response.data.primary_risk_factors || [],
        reasoning: toSingleLineReasoning(
          response.data.reasoning || response.data.output_2?.reasoning || ''
        ),
        outputId: response.data.output_id || null,
        totalTokens: Number.isFinite(Number(response.data.total_tokens))
          ? Number(response.data.total_tokens)
          : null,
        inputTokens: Number.isFinite(Number(response.data.input_tokens))
          ? Number(response.data.input_tokens)
          : null,
        model: response.data.model || null,
        // Persist the exact text and masking used at alert-time.
        analysisInput: textToUse,
        maskedInput: maskedToUse,
        originalInput: textToUse,
        output1: response.data.output_1 || {},
        output2: response.data.output_2 || {}
      };

      if (!silent) {
        setWarningState(assessment);
        setLastRiskAnalysis(assessment);
        setLastAssessedText(textToUse);
        pendingAbortMarkerRef.current = false;

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
        setRiskError(true);
        // Keep modal open — WarningModal will show error state with retry
      }
      return null;
    } finally {
      if (pendingRiskOutputIdRef.current === outputId) {
        pendingRiskOutputIdRef.current = null;
      }
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

  const handleRetryAssessment = async () => {
    setWarningState(null);
    setCapReached(false);
    setRiskError(false);
    setRiskPending(true);
    const textToUse = draftText.trim();
    if (!textToUse) return;
    const assessment = await assessRisk(textToUse, { openOnComplete: true, forcePiiRefresh: false });
    if (assessment) {
      const interactionRow = await startAlertInteraction();
      if (interactionRow) {
        await completeAlertInteraction(interactionRow.id, assessment);
      }
    }
  };

  const handleOpenWarning = async () => {
    const textToUse = draftText.trim();
    if (!textToUse) {
      return;
    }
    if (riskPending) {
      return;
    }
    setIsDrawerOpen(false);
    suppressAutoOpenRef.current = false;
    setRiskError(false);
    setCapReached(false);

    // Stop pending typing debounce before explicit analysis click.
    clearLiveTimers();
    livePipelineVersionRef.current += 1;

    const reusableAssessment = lastAssessedText === textToUse
      ? (warningState || lastRiskAnalysis)
      : null;

    setWarningState(null);
    setLastOfferedRewrite(null);
    setLastShownRewrite(null);
    setRiskPending(true);
    setIsWarningOpen(true);

    const interactionRow = await startAlertInteraction();
    if (!interactionRow) {
      setRiskPending(false);
      setIsWarningOpen(false);
      return;
    }

    if (reusableAssessment) {
      const completed = await completeAlertInteraction(interactionRow.id, reusableAssessment);
      if (!completed) {
        setRiskPending(false);
        setIsWarningOpen(false);
        return;
      }
      setWarningState(reusableAssessment);
      setLastRiskAnalysis(reusableAssessment);
      setRiskPending(false);
      pendingAbortMarkerRef.current = false;
      return;
    }

    const assessment = await assessRisk(textToUse, {
      openOnComplete: true,
      forcePiiRefresh: true
    });
    if (assessment) {
      const completed = await completeAlertInteraction(interactionRow.id, assessment);
      if (!completed) {
        setRiskPending(false);
        setIsWarningOpen(false);
        return;
      }
    }
    if (assessment?.saferRewrite) {
      setLastShownRewrite(assessment.saferRewrite);
    }
  };

  const handleSend = async () => {
    if (!draftText.trim() || sendInFlightRef.current) return;

    sendInFlightRef.current = true;
    setIsSending(true);
    setIsSubmitTransitioning(false);
    if (submitLoadingDelayRef.current) {
      clearTimeout(submitLoadingDelayRef.current);
      submitLoadingDelayRef.current = null;
    }
    submitLoadingDelayRef.current = setTimeout(() => {
      setIsSubmitTransitioning(true);
      submitLoadingDelayRef.current = null;
    }, 1000);
    setIsDrawerOpen(false);
    clearLiveTimers();
    livePipelineVersionRef.current += 1;
    riskRequestCounterRef.current += 1;
    abortActiveAssessRequests();

    const finalText = draftText.trim();

    const newMessage = {
      id: `sent-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
      text: finalText,
      direction: 'SENT',
      timestamp: new Date()
    };

    // Optimistic UI update keeps send interaction responsive.
    setMessages((prev) => [...prev, newMessage]);
    setWarningState(null);
    setLastRiskAnalysis(null);
    setRiskPending(false);
    setRiskError(false);
    setCapReached(false);
    setLastOfferedRewrite(null);
    setLastShownRewrite(null);
    setIsWarningOpen(false);
    setLastAssessedText('');
    setPiiSpans([]);
    setCurrentMessageIndex((prev) => prev + 1);

    try {
      const messagePayload = {
        participant_id: participantProlificId,
        conversation_index: conversationIndex,
        final_message: finalText,
        variant
      };

      await axios.post(`${API_BASE_URL}/api/participants/message`, messagePayload);
      pendingAbortMarkerRef.current = false;
      // Only clear draft after confirmed success
      setDraftText('');
      try { sessionStorage.removeItem(draftStorageKey); } catch (_) { /* ignore */ }
    } catch (error) {
      if (navigateOnStepConflict(error)) {
        if (submitLoadingDelayRef.current) {
          clearTimeout(submitLoadingDelayRef.current);
          submitLoadingDelayRef.current = null;
        }
        setIsSubmitTransitioning(false);
        return;
      }
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
        navigate(`/survey/mid?index=${conversationIndex}`, { replace: true });
      }, 2000);
      return;
    }

    setIsSubmitTransitioning(false);
  };

  const handleAcceptRewrite = () => {
    if (warningState && warningState.saferRewrite) {
      const rewriteText = warningState.saferRewrite;
      void recordAlertDecision(true);
      pendingAbortMarkerRef.current = false;
      // Clear stale underline immediately, then route through the normal
      // typing pipeline so GLiNER re-runs on the accepted rewrite.
      setPiiSpans([]);
      handleTyping(rewriteText, { preserveDecision: true });
      setLastShownRewrite(rewriteText);
    }
    setIsWarningOpen(false);
  };

  const handleContinueAnyway = () => {
    if (riskPending) {
      // User closed before LLM returned: treat this as no completed assessment.
      // This ensures submit persists ABORT markers for assessment-derived fields.
      const abortedOutputId = pendingRiskOutputIdRef.current;
      pendingRiskOutputIdRef.current = null;
      suppressAutoOpenRef.current = true;
      riskRequestCounterRef.current += 1;
      abortActiveAssessRequests();
      if (abortedOutputId && participantProlificId) {
        void axios.post(`${API_BASE_URL}/api/risk/abort`, {
          participant_prolific_id: participantProlificId,
          session_id: conversationIndex + 1,
          output_id: abortedOutputId,
          scenario_response_id: activeAlertInteractionRef.current?.id || null
        }).catch((error) => {
          console.warn('[RISK] Failed to log aborted output:', error);
        });
      }
      setRiskPending(false);
      setWarningState(null);
      setLastOfferedRewrite(null);
      setLastShownRewrite(null);
      setPiiSpans([]);
      pendingAbortMarkerRef.current = true;
    } else {
      void recordAlertDecision(false);
      pendingAbortMarkerRef.current = false;
    }
    clearLiveTimers();
    setIsWarningOpen(false);
  };

  // Warn user if they try to close/refresh while a send is in flight
  useEffect(() => {
    const handler = (e) => {
      if (sendInFlightRef.current) {
        e.preventDefault();
        e.returnValue = '';
      }
    };
    window.addEventListener('beforeunload', handler);
    return () => window.removeEventListener('beforeunload', handler);
  }, []);

  useEffect(() => () => {
    if (submitLoadingDelayRef.current) {
      clearTimeout(submitLoadingDelayRef.current);
      submitLoadingDelayRef.current = null;
    }
    clearLiveTimers();
    abortActiveAssessRequests();
  }, []);

  useEffect(() => {
    if (!isSubmitTransitioning) {
      setSubmitLoadingText('Loading');
      return undefined;
    }
    const frames = ['Loading', 'Loading.', 'Loading..', 'Loading...'];
    let index = 0;
    setSubmitLoadingText(frames[0]);
    const interval = setInterval(() => {
      index = (index + 1) % frames.length;
      setSubmitLoadingText(frames[index]);
    }, 450);
    return () => clearInterval(interval);
  }, [isSubmitTransitioning]);

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

  if (isSubmitTransitioning) {
    return <div className="loading">{submitLoadingText}</div>;
  }

  return (
    <div
      className="conversation-screen"
      style={{ '--composer-height': `${composerHeight}px` }}
    >
      <ChatHeader contactName={contactName} scenario={conversation.scenario} />
      <div className="conversation-body">
        <MessageList messages={messages} conversationKey={conversationIndex} scrollLocked={isDrawerOpen} />

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
      <div ref={composerContainerRef} className="conversation-composer-layer">
        <ChatComposer
          draftText={draftText}
          onTextChange={handleTyping}
          onSend={handleSend}
          variant={variant}
          piiSpans={piiSpans}
          onPiiClick={handleOpenWarning}
          isSending={isSending}
          sendDisabled={isWarningOpen}
          inputDisabled={isWarningOpen}
        />
      </div>
      {isWarningOpen && (
        <WarningModal
          warningState={warningState}
          riskPending={riskPending}
          riskError={riskError}
          capReached={capReached}
          onAcceptRewrite={handleAcceptRewrite}
          onContinueAnyway={handleContinueAnyway}
          onRetry={handleRetryAssessment}
        />
      )}
    </div>
  );
}

export default ConversationScreen;
