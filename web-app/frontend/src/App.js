import React, { useState, useEffect, useCallback } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate, useParams } from 'react-router-dom';
import axios from 'axios';
import WelcomeScreen from './components/WelcomeScreen';
import ConversationScreen from './components/ConversationScreen';
import SurveyScreen from './components/SurveyScreen';
import CompletionScreen from './components/CompletionScreen';
import AlreadyCompletedScreen from './components/AlreadyCompletedScreen';
import AdminParticipantView from './components/AdminParticipantView';
import './App.css';

const API_BASE_URL = process.env.REACT_APP_BACKEND_BASE_URL || 'http://localhost:8080';
const PROLIFIC_STORAGE_KEY = 'whatsapp_prolific_id';

// Hardcoded conversation data (from annotated_test.json)
// This will be loaded from the backend in production
const SEED_CONVERSATIONS = [
  {
    conversation_id: 1000,
    scenario: "Academic Collaboration",
    conversation: [],
    ground_truth: {}
  },
  {
    conversation_id: 1001,
    scenario: "Professional Networking",
    conversation: [],
    ground_truth: {}
  },
  {
    conversation_id: 1002,
    scenario: "Job Recruitment",
    conversation: [],
    ground_truth: {}
  }
];

const generateLocalProlificId = () => {
  const randomPart = Math.random().toString(36).slice(2, 8);
  return `local_${Date.now()}_${randomPart}`;
};

const shouldTagVariantInPid = (pid) => /^(local|test)_/i.test(pid || '');

const normalizeProlificId = (pid) => {
  const cleaned = (pid || '').trim();
  if (!cleaned) return '';
  if (!shouldTagVariantInPid(cleaned)) return cleaned;
  return cleaned.replace(/_([AB])$/i, '');
};

const withVariantSuffix = (pid, assignedVariant) => {
  const base = normalizeProlificId(pid);
  if (!base || !assignedVariant) return base;
  return `${base}_${assignedVariant}`;
};

const resolveProlificId = () => {
  const urlParams = new URLSearchParams(window.location.search);
  const urlProlificId = urlParams.get('PROLIFIC_PID');
  if (urlProlificId) {
    const canonicalId = normalizeProlificId(urlProlificId);
    localStorage.setItem(PROLIFIC_STORAGE_KEY, canonicalId);
    return canonicalId;
  }

  const storedId = localStorage.getItem(PROLIFIC_STORAGE_KEY);
  if (storedId) {
    const canonicalId = normalizeProlificId(storedId);
    if (canonicalId !== storedId) {
      localStorage.setItem(PROLIFIC_STORAGE_KEY, canonicalId);
    }
    return canonicalId;
  }

  const generatedId = generateLocalProlificId();
  localStorage.setItem(PROLIFIC_STORAGE_KEY, generatedId);
  return generatedId;
};

// Component to handle conversation route with index parameter
function ConversationRoute({ conversations, participantId, prolificId, variant, onComplete }) {
  const { index } = useParams();
  const conversationIndex = parseInt(index || '0', 10);
  
  if (conversations.length === 0) {
    return <div className="loading">Loading conversations...</div>;
  }
  
  if (conversationIndex >= conversations.length || conversationIndex < 0) {
    return <Navigate to="/completion" />;
  }
  
  return (
    <ConversationScreen
      conversation={conversations[conversationIndex]}
      sessionId={conversationIndex + 1}  // Simple session ID based on index
      participantId={participantId}
      participantProlificId={prolificId}
      variant={variant}
      onComplete={onComplete}
      conversationIndex={conversationIndex}
    />
  );
}

function App() {
  const isAdminRoute = window.location.pathname.startsWith('/admin');
  const [participantId, setParticipantId] = useState(null);
  const [variant, setVariant] = useState(null);
  const [participantStatus, setParticipantStatus] = useState('new');
  const [completionUrl, setCompletionUrl] = useState('');
  const [prolificId, setProlificId] = useState('');
  const [currentConversationIndex, setCurrentConversationIndex] = useState(0);
  const [conversations, setConversations] = useState([]);
  const [loading, setLoading] = useState(!isAdminRoute);
  const [initialized, setInitialized] = useState(false);
  const [piiReady, setPiiReady] = useState(true);

  const initializeParticipant = useCallback(async () => {
    try {
      // Get prolific ID from URL params if present
      const prolificId = resolveProlificId();

      const response = await axios.post(`${API_BASE_URL}/api/participants`, {
        prolific_id: prolificId
      });
      
      setParticipantId(response.data.id);
      setVariant(response.data.variant);
      setParticipantStatus(response.data.status || 'new');
      setCompletionUrl(response.data.completion_url || '');
      setProlificId(prolificId);

      if (shouldTagVariantInPid(prolificId) && response.data.variant) {
        const taggedPid = withVariantSuffix(prolificId, response.data.variant);
        const currentUrl = new URL(window.location.href);
        if (currentUrl.searchParams.get('PROLIFIC_PID') !== taggedPid) {
          currentUrl.searchParams.set('PROLIFIC_PID', taggedPid);
          const nextSearch = currentUrl.searchParams.toString();
          const nextUrl = `${currentUrl.pathname}${nextSearch ? `?${nextSearch}` : ''}${currentUrl.hash || ''}`;
          window.history.replaceState({}, '', nextUrl);
        }
      }
    } catch (error) {
      console.error('Error initializing participant:', error);
    } finally {
      setLoading(false);
    }
  }, []);

  const loadConversations = useCallback(async () => {
    try {
      // Try to load from backend first
      const response = await axios.get(`${API_BASE_URL}/api/conversations/seed`);
      setConversations(response.data);
    } catch (error) {
      console.warn('Could not load conversations from backend, using seed data');
      // Fall back to seed data (in a real app, this would come from the backend)
      setConversations(SEED_CONVERSATIONS);
    }
  }, []);

  const checkPiiStatus = useCallback(async () => {
    if (variant !== 'A') {
      setPiiReady(true);
      return;
    }
    try {
      const response = await axios.get(`${API_BASE_URL}/pii/status`);
      setPiiReady(Boolean(response.data?.loaded));
    } catch (error) {
      console.error('Error checking PII status:', error);
      setPiiReady(false);
    }
  }, [variant]);

  useEffect(() => {
    if (isAdminRoute) {
      setLoading(false);
      return;
    }
    // Initialize participant only once
    if (!initialized) {
      initializeParticipant();
      loadConversations();
      setInitialized(true);
    }
  }, [initialized, isAdminRoute, initializeParticipant, loadConversations]);

  useEffect(() => {
    if (isAdminRoute) {
      return undefined;
    }
    if (variant === 'A') {
      checkPiiStatus();
      const interval = setInterval(checkPiiStatus, 3000);
      return () => clearInterval(interval);
    }
    return undefined;
  }, [variant, isAdminRoute, checkPiiStatus]);

  const handleConversationComplete = () => {
    if (currentConversationIndex < conversations.length - 1) {
      setCurrentConversationIndex(currentConversationIndex + 1);
    }
  };

  if (isAdminRoute) {
    return (
      <Router>
        <div className="App">
          <Routes>
            <Route path="/admin" element={<AdminParticipantView />} />
            <Route path="/admin/*" element={<AdminParticipantView />} />
          </Routes>
        </div>
      </Router>
    );
  }

  if (loading) {
    return <div className="loading">Loading...</div>;
  }

  if (participantStatus === 'completed') {
    return <AlreadyCompletedScreen completionUrl={completionUrl} />;
  }

  if (!participantId) {
    return <div className="error">Error initializing participant</div>;
  }

  if (variant === 'A' && !piiReady) {
    return <div className="loading">Loading PII model...</div>;
  }

  return (
    <Router>
      <div className="App">
        <Routes>
          <Route 
            path="/" 
            element={
              <WelcomeScreen 
                prolificId={prolificId}
                variant={variant}
              />
            } 
          />
          <Route 
            path="/conversation/:index" 
            element={
              <ConversationRoute
                conversations={conversations}
                participantId={participantId}
                prolificId={prolificId}
                variant={variant}
                onComplete={handleConversationComplete}
              />
            } 
          />
          <Route 
            path="/survey/:type" 
            element={
              <SurveyScreen
                participantId={participantId}
                participantProlificId={prolificId}
                variant={variant}
              />
            } 
          />
          <Route 
            path="/completion" 
            element={
              <CompletionScreen
                participantId={participantId}
                prolificId={prolificId}
              />
            } 
          />
        </Routes>
      </div>
    </Router>
  );
}

export default App;
