import React, { useEffect, useState } from 'react';
import './WarningModal.css';

function WarningModal({ warningState, riskPending, onAcceptRewrite, onContinueAnyway }) {
  const [loadingDots, setLoadingDots] = useState('.');
  const rewriteText = warningState?.saferRewrite || '';
  const reasoning = warningState?.reasoning || '';
  const riskLevel = warningState?.riskLevel || 'UNKNOWN';
  const disableAccept = riskPending || !rewriteText.trim();

  useEffect(() => {
    if (!riskPending) {
      setLoadingDots('.');
      return undefined;
    }
    const frames = ['.', '..', '...'];
    let index = 0;
    const timer = setInterval(() => {
      index = (index + 1) % frames.length;
      setLoadingDots(frames[index]);
    }, 350);
    return () => clearInterval(timer);
  }, [riskPending]);

  return (
    <div className="warning-modal-overlay">
      <div className="warning-modal">
        <div className="warning-content">
          {riskPending ? (
            <div className="warning-explanation">Loading analysis{loadingDots}</div>
          ) : (
            <>
              <div className="risk-block">
                <h3>Risk Level</h3>
                <p className={`risk-level risk-level-${riskLevel.toLowerCase()}`}>{riskLevel}</p>
              </div>
              <div className="rewrite-block">
                <h3>Suggested Rewrite</h3>
                <div className="rewrite-text">{rewriteText || 'No rewrite available.'}</div>
              </div>
              <div className="reasoning-block">
                <h3>Reasoning</h3>
                <p className="warning-explanation">{reasoning || 'This rewrite reduces sensitive detail exposure.'}</p>
              </div>
            </>
          )}
        </div>
        
        <div className="warning-actions">
          <button 
            className="continue-button"
            onClick={onContinueAnyway}
          >
            Continue anyway
          </button>
          <button 
            className="accept-button"
            onClick={onAcceptRewrite}
            disabled={disableAccept}
          >
            Accept safer rewrite
          </button>
        </div>
      </div>
    </div>
  );
}

export default WarningModal;
