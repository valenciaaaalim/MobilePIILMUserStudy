import React from 'react';
import { useLocation } from 'react-router-dom';

const STEPS = [
  { path: '/', label: 'Consent' },
  { path: '/survey/baseline', label: 'Baseline' },
  { path: '/conversation/0', label: 'Scenario 1' },
  { path: '/survey/mid', label: 'Survey 1' },
  { path: '/conversation/1', label: 'Scenario 2' },
  { path: '/survey/mid', label: 'Survey 2' },
  { path: '/conversation/2', label: 'Scenario 3' },
  { path: '/survey/mid', label: 'Survey 3' },
  { path: '/survey/end-of-study', label: 'Final Survey' },
  { path: '/completion', label: 'Done' },
];

function resolveStepIndex(pathname, search) {
  if (pathname === '/') return 0;
  if (pathname === '/survey/baseline') return 1;
  if (pathname.startsWith('/conversation/')) {
    const idx = parseInt(pathname.split('/')[2] || '0', 10);
    return 2 + idx * 2; // 2, 4, 6
  }
  if (pathname === '/survey/mid' || pathname === '/survey/post-scenario') {
    const idx = new URLSearchParams(search).get('index');
    const convIdx = parseInt(idx || '0', 10);
    return 3 + convIdx * 2; // 3, 5, 7
  }
  if (pathname === '/survey/end-of-study' || pathname === '/survey/post') return 8;
  if (pathname === '/completion') return 9;
  return -1;
}

function StudyProgressBar() {
  const location = useLocation();
  const currentStep = resolveStepIndex(location.pathname, location.search);

  if (currentStep < 0) return null;

  const progress = Math.round((currentStep / (STEPS.length - 1)) * 100);

  return (
    <div style={{
      background: '#f5f5f5',
      padding: 'calc(env(safe-area-inset-top, 0px) + 6px) 12px 4px',
      borderBottom: '1px solid #e0e0e0',
      flexShrink: 0,
    }}>
      <div style={{
        height: '4px', borderRadius: '2px',
        background: '#e0e0e0', overflow: 'hidden',
      }}>
        <div style={{
          height: '100%', borderRadius: '2px',
          background: '#4caf50',
          width: `${progress}%`,
          transition: 'width 0.3s ease',
        }} />
      </div>
      <div style={{
        fontSize: '11px', color: '#666',
        marginTop: '2px', textAlign: 'center',
      }}>
        Step {currentStep + 1} of {STEPS.length}
      </div>
    </div>
  );
}

export default StudyProgressBar;
