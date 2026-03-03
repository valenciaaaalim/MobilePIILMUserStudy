import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';
import './WelcomeScreen.css';
import consentFormPdf from './003_OnlineSurvey_Consent_MobilePIILM.pdf';

const API_BASE_URL = process.env.REACT_APP_BACKEND_BASE_URL || 'http://localhost:8080';

function WelcomeScreen({ prolificId }) {
  const navigate = useNavigate();
  const [submitting, setSubmitting] = useState(false);
  const [hasReachedBottom, setHasReachedBottom] = useState(false);
  const scrollContainerRef = useRef(null);

  const logConsent = async (consent) => {
    await axios.post(`${API_BASE_URL}/api/consent`, {
      consent,
      participant_platform_id: prolificId || null
    });
  };

  const updateScrollState = useCallback(() => {
    const scrollElement = scrollContainerRef.current;
    if (!scrollElement) return;

    const scrollThreshold = 8;
    const reachedBottom =
      scrollElement.scrollTop + scrollElement.clientHeight >=
      scrollElement.scrollHeight - scrollThreshold;

    if (reachedBottom) {
      setHasReachedBottom(true);
    }
  }, []);

  useEffect(() => {
    updateScrollState();
    window.addEventListener('resize', updateScrollState);
    return () => window.removeEventListener('resize', updateScrollState);
  }, [updateScrollState]);

  const handleContinue = async () => {
    if (!hasReachedBottom || submitting) return;

    setSubmitting(true);
    try {
      await logConsent('yes');
      navigate('/survey/pre');
    } catch (error) {
      console.error('Error logging consent:', error);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="consent-screen">
      <div className="consent-shell">
        <div
          className="consent-scroll"
          ref={scrollContainerRef}
          onScroll={updateScrollState}
        >
          <section className="consent-intro">
            <h1>Understanding How Privacy Warnings Prevent Disclosure of Personal Information While Messaging</h1>
          </section>

          <section className="information-sheet">
            <h2>Study Summary</h2>
            <p className="spaced-paragraph">
            You will complete a short smartphone study about how people respond to in-the-moment privacy-related feedback while typing messages. You will read fictional, preloaded chat conversations and type brief replies using provided reference text. Some participants may see automated feedback while typing, while others will not.            
            </p>
            <h2>Privacy Risks and Discomforts</h2>
            <p className="spaced-paragraph">
            The study involves minimal or unlikely risks. Do not enter your real personal details. We will only collect your Prolific Participant ID for payment and completion, plus interaction logs and survey responses. Data is stored securely and reported only in aggregate.      
            </p>
            <h2>Voluntary Participation and Withdrawal</h2>
            <p className="spaced-paragraph">
            Participation is voluntary. You may stop at any time. If you withdraw before completing the study, you will not receive compensation and your responses will not be analyzed.            
            </p>
            <hr className="info-divider" />
            <h2>Consent</h2>
            <p className="spaced-paragraph">
            By clicking <strong>Continue</strong>, you confirm you are eligible, have read this summary and the full information sheet, and consent to participate.            
            </p>
            
            
          <p style={{ marginTop: '18px' }}>
            <a
              href={consentFormPdf}
              download="003_OnlineSurvey_Consent_MobilePIILM.pdf"
              rel="noopener noreferrer"
              style={{ textDecoration: 'underline', color: '#2266bb', fontWeight: 500 }}
            >
              Download full Participant Information Sheet and Consent Form.
            </a>
          </p>

            
            
          </section>
        </div>
        <div className="consent-fade" />
        <div className="consent-footer">
          <button
            className={`consent-continue${hasReachedBottom ? ' ready' : ''}`}
            onClick={handleContinue}
            disabled={!hasReachedBottom || submitting}
          >
            Continue
          </button>
        </div>
      </div>
    </div>
  );
}

export default WelcomeScreen;
