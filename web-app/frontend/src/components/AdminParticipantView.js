import React, { useState } from 'react';
import axios from 'axios';
import './AdminParticipantView.css';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

function formatLabel(key) {
  return key
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function formatValue(value) {
  if (value === null || value === undefined || value === '') {
    return 'N/A';
  }
  if (Array.isArray(value) || typeof value === 'object') {
    return JSON.stringify(value);
  }
  return String(value);
}

function RecordView({ record }) {
  const entries = Object.entries(record || {});
  if (entries.length === 0) {
    return <div className="admin-empty">No data</div>;
  }

  return (
    <div className="admin-record-grid">
      {entries.map(([key, value]) => (
        <div className="admin-record-item" key={key}>
          <div className="admin-record-key">{formatLabel(key)}</div>
          <div className="admin-record-value">{formatValue(value)}</div>
        </div>
      ))}
    </div>
  );
}

function RecordList({ records, labelPrefix }) {
  if (!Array.isArray(records) || records.length === 0) {
    return <div className="admin-empty">No records</div>;
  }

  return (
    <div className="admin-list">
      {records.map((record, index) => {
        const scenario = record?.scenario_number ? ` - Scenario ${record.scenario_number}` : '';
        return (
          <div className="admin-list-card" key={`${labelPrefix}-${record?.id || index}`}>
            <div className="admin-list-title">
              {labelPrefix} {index + 1}{scenario}
            </div>
            <RecordView record={record} />
          </div>
        );
      })}
    </div>
  );
}

function AdminParticipantView() {
  const [prolificIdInput, setProlificIdInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [data, setData] = useState(null);

  const handleSearch = async (event) => {
    event.preventDefault();
    const trimmed = prolificIdInput.trim();
    if (!trimmed) {
      setError('Please enter a Prolific ID.');
      return;
    }

    setLoading(true);
    setError('');
    setData(null);

    try {
      const response = await axios.get(
        `${API_BASE_URL}/api/participants/by-prolific/${encodeURIComponent(trimmed)}/data`
      );
      setData(response.data);
    } catch (requestError) {
      if (requestError?.response?.status === 404) {
        setError(`No participant found for Prolific ID: ${trimmed}`);
      } else {
        setError('Failed to load participant data. Please check backend connectivity and try again.');
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="admin-view">
      <div className="admin-header">
        <h1>Participant Response Viewer</h1>
        <p>Enter a Prolific ID to view all captured responses across the study.</p>
      </div>

      <form className="admin-search" onSubmit={handleSearch}>
        <input
          type="text"
          value={prolificIdInput}
          onChange={(event) => setProlificIdInput(event.target.value)}
          placeholder="e.g. test_1771579373_16742"
        />
        <button type="submit" disabled={loading}>
          {loading ? 'Loading...' : 'Load Responses'}
        </button>
      </form>

      {error && <div className="admin-error">{error}</div>}

      {data && (
        <div className="admin-sections">
          <section className="admin-section">
            <h2>Participant</h2>
            <RecordView record={data.participant} />
          </section>

          <section className="admin-section">
            <h2>Baseline Assessment</h2>
            {data.baseline_assessment ? (
              <RecordView record={data.baseline_assessment} />
            ) : (
              <div className="admin-empty">No baseline response</div>
            )}
          </section>

          <section className="admin-section">
            <h2>Scenario Responses</h2>
            <RecordList records={data.scenario_responses} labelPrefix="Response" />
          </section>

          <section className="admin-section">
            <h2>Post-Scenario Surveys</h2>
            <RecordList records={data.post_scenario_surveys} labelPrefix="Survey" />
          </section>

          <section className="admin-section">
            <h2>PII Disclosures</h2>
            <RecordList records={data.pii_disclosures} labelPrefix="Disclosure" />
          </section>

          <section className="admin-section">
            <h2>SUS Responses</h2>
            {data.sus_responses ? (
              <RecordView record={data.sus_responses} />
            ) : (
              <div className="admin-empty">No SUS responses</div>
            )}
          </section>

          <section className="admin-section">
            <h2>End-of-Study Survey</h2>
            {data.end_of_study_survey ? (
              <RecordView record={data.end_of_study_survey} />
            ) : (
              <div className="admin-empty">No end-of-study response</div>
            )}
          </section>
        </div>
      )}
    </div>
  );
}

export default AdminParticipantView;
