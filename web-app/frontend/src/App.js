import React, { useState, useEffect, useCallback, useRef } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate, Outlet, useLocation, useParams } from 'react-router-dom';
import axios from 'axios';
import WelcomeScreen from './components/WelcomeScreen';
import ConversationScreen from './components/ConversationScreen';
import SurveyScreen from './components/SurveyScreen';
import CompletionScreen from './components/CompletionScreen';
import AlreadyCompletedScreen from './components/AlreadyCompletedScreen';
import AdminParticipantView from './components/AdminParticipantView';
import StudyProgressBar from './components/StudyProgressBar';
import './App.css';

const API_BASE_URL = process.env.REACT_APP_BACKEND_BASE_URL || 'http://localhost:8080';
const PROLIFIC_STORAGE_KEY = 'whatsapp_prolific_id';

function isMobileDevice() {
  const ua = navigator.userAgent || '';
  if (/Android|iPhone|iPod|iPad|BlackBerry|IEMobile|Opera Mini/i.test(ua)) return true;
  return window.innerWidth <= 768 && 'ontouchstart' in window;
}

// Hardcoded conversation data (from conversation_history.json)
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

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function AnimatedLoading({ baseText = 'Loading' }) {
  const [loadingText, setLoadingText] = useState(baseText);

  useEffect(() => {
    const frames = [baseText, `${baseText}.`, `${baseText}..`, `${baseText}...`];
    let index = 0;
    const interval = setInterval(() => {
      index = (index + 1) % frames.length;
      setLoadingText(frames[index]);
    }, 450);
    return () => clearInterval(interval);
  }, [baseText]);

  return <div className="loading">{loadingText}</div>;
}

// Component to handle conversation route with index parameter
function ConversationRoute({ conversations, participantId, prolificId, variant, onComplete }) {
  const { index } = useParams();
  const conversationIndex = parseInt(index || '0', 10);
  
  if (conversations.length === 0) {
    return <AnimatedLoading baseText="Loading conversations" />;
  }
  
  if (conversationIndex >= conversations.length || conversationIndex < 0) {
    return <Navigate to="/completion" />;
  }
  
  return (
    <ConversationScreen
      conversation={conversations[conversationIndex]}
      participantId={participantId}
      participantProlificId={prolificId}
      variant={variant}
      onComplete={onComplete}
      conversationIndex={conversationIndex}
    />
  );
}

function routeMatchesAllowed(location, allowedPath) {
  if (!allowedPath) return false;
  const currentPathWithSearch = `${location.pathname}${location.search || ''}`;
  if (allowedPath.includes('?')) {
    return currentPathWithSearch === allowedPath;
  }
  return location.pathname === allowedPath;
}

function StudyGuard({ participantId }) {
  const location = useLocation();
  const [checkingProgress, setCheckingProgress] = useState(true);
  const [progressError, setProgressError] = useState('');
  const [progress, setProgress] = useState(null);

  useEffect(() => {
    let cancelled = false;
    const loadProgress = async () => {
      if (!participantId) return;
      setCheckingProgress(true);
      try {
        const response = await axios.get(`${API_BASE_URL}/api/participants/${participantId}/progress`);
        if (cancelled) return;
        setProgress(response.data);
        setProgressError('');
      } catch (error) {
        if (cancelled) return;
        console.error('[StudyGuard] Failed to load participant progress:', error);
        setProgressError('Could not verify study progress.');
      } finally {
        if (!cancelled) {
          setCheckingProgress(false);
        }
      }
    };
    loadProgress();
    return () => {
      cancelled = true;
    };
  }, [participantId, location.pathname, location.search]);

  if (progressError) {
    return <div className="error">{progressError}</div>;
  }

  if (checkingProgress || !progress) {
    return <AnimatedLoading baseText="Checking progress" />;
  }

  const allowedPaths = progress.allowed_paths?.length ? progress.allowed_paths : [progress.redirect_path];
  const isAllowed = allowedPaths.some((path) => routeMatchesAllowed(location, path));
  if (!isAllowed) {
    return <Navigate to={progress.redirect_path} replace />;
  }

  return <Outlet />;
}

function App() {
  const isAdminRoute = window.location.pathname.startsWith('/admin');
  const [participantId, setParticipantId] = useState(null);
  const [variant, setVariant] = useState(null);
  const [participantStatus, setParticipantStatus] = useState('new');
  const [completionUrl, setCompletionUrl] = useState('');
  const [prolificId, setProlificId] = useState('');
  const [sessionToken, setSessionToken] = useState(null);
  const [conversations, setConversations] = useState([]);
  const [loading, setLoading] = useState(!isAdminRoute);
  const [initialized, setInitialized] = useState(false);
  const [piiReady, setPiiReady] = useState(true);
  const [initialPiiDelayDone, setInitialPiiDelayDone] = useState(false);

  const initializeParticipant = useCallback(async () => {
    let lastError = null;
    try {
      // Get prolific ID from URL params if present
      const prolificId = resolveProlificId();
      const attemptDelaysMs = [0, 1200, 2500];

      for (let attempt = 0; attempt < attemptDelaysMs.length; attempt += 1) {
        if (attemptDelaysMs[attempt] > 0) {
          await sleep(attemptDelaysMs[attempt]);
        }
        try {
          const response = await axios.post(`${API_BASE_URL}/api/participants`, {
            prolific_id: prolificId
          }, {
            timeout: 15000
          });

          setParticipantId(response.data.id);
          setVariant(response.data.variant);
          setParticipantStatus(response.data.status || 'new');
          setCompletionUrl(response.data.completion_url || '');
          setProlificId(prolificId);
          if (response.data.session_token) {
            axios.defaults.headers.common['X-Session-Token'] = response.data.session_token;
            setSessionToken(response.data.session_token);
          }
          return;
        } catch (error) {
          lastError = error;
          const status = error?.response?.status;
          const shouldRetry = !status || status >= 500 || status === 429;
          if (!shouldRetry || attempt === attemptDelaysMs.length - 1) {
            throw error;
          }
        }
      }
    } catch (error) {
      console.error('Error initializing participant:', lastError || error);
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
      return true;
    }
    try {
      const response = await axios.get(`${API_BASE_URL}/pii/status`);
      const loaded = Boolean(response.data?.loaded);
      setPiiReady(loaded);
      return loaded;
    } catch (error) {
      console.error('Error checking PII status:', error);
      setPiiReady(false);
      return false;
    }
  }, [variant]);

  const waitForPiiReady = useCallback(async () => {
    if (variant !== 'A') {
      return true;
    }

    while (true) {
      const loaded = await checkPiiStatus();
      if (loaded) {
        return true;
      }
      await sleep(1500);
    }
  }, [variant, checkPiiStatus]);

  useEffect(() => {
    if (isAdminRoute) {
      setLoading(false);
      return;
    }
    // Block participant creation for non-mobile users
    if (!isMobileDevice()) {
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

  useEffect(() => {
    if (isAdminRoute) {
      return undefined;
    }
    if (!variant) {
      return undefined;
    }
    if (variant !== 'A') {
      setInitialPiiDelayDone(true);
      return undefined;
    }

    setInitialPiiDelayDone(false);
    const timer = setTimeout(() => {
      setInitialPiiDelayDone(true);
    }, 6000);

    return () => clearTimeout(timer);
  }, [variant, isAdminRoute]);

  useEffect(() => {
    if (sessionToken) {
      axios.defaults.headers.common['X-Session-Token'] = sessionToken;
    }
  }, [sessionToken]);

  const appRef = useRef(null);

  useEffect(() => {
    const vv = window.visualViewport;
    if (!vv) return undefined;
    const update = () => {
      const h = `${vv.height}px`;
      document.documentElement.style.height = h;
      document.body.style.height = h;
      const root = document.getElementById('root');
      if (root) root.style.height = h;
      if (appRef.current) {
        appRef.current.style.height = h;
      }
      // Keep viewport pinned to top-left to prevent iOS scroll offset
      window.scrollTo(0, 0);
    };
    const onScroll = () => {
      if (vv.offsetTop !== 0) {
        window.scrollTo(0, 0);
      }
    };
    update();
    vv.addEventListener('resize', update);
    vv.addEventListener('scroll', onScroll);
    return () => {
      vv.removeEventListener('resize', update);
      vv.removeEventListener('scroll', onScroll);
    };
  }, []);

  const handleConversationComplete = () => {
    // Navigation is handled by the router; this is a no-op kept for prop interface compatibility.
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

  if (!isMobileDevice()) {
    return (
      <div className="error" style={{ padding: '2rem', textAlign: 'center' }}>
        <p>This study must be completed on a <strong>mobile device</strong>.</p>
        <p> Please open this link on your smartphone.</p>
      </div>
    );
  }

  if (loading) {
    return <AnimatedLoading />;
  }

  if (participantStatus === 'completed') {
    return <AlreadyCompletedScreen completionUrl={completionUrl} />;
  }

  if (!participantId) {
    return <div className="error">Error initializing participant</div>;
  }

  if (variant === 'A' && !initialPiiDelayDone) {
    return <AnimatedLoading />;
  }

  return (
    <Router>
      <div className="App" ref={appRef}>
        <StudyProgressBar />
        <Routes>
          <Route element={<StudyGuard participantId={participantId} />}>
            <Route 
              path="/" 
              element={
                <WelcomeScreen
                  prolificId={prolificId}
                  variant={variant}
                  piiReady={piiReady}
                  waitForPiiReady={waitForPiiReady}
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
          </Route>
        </Routes>
      </div>
    </Router>
  );
}

export default App;
