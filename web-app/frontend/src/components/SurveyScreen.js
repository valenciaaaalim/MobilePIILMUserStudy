import React, { useState, useEffect, useRef } from 'react';
import { useNavigate, useSearchParams, useParams } from 'react-router-dom';
import axios from 'axios';
import './SurveyScreen.css';
import { getRedirectPathFrom409 } from '../utils/apiErrors';

const API_BASE_URL = process.env.REACT_APP_BACKEND_BASE_URL || 'http://localhost:8080';
const REDIRECT_ABORT = 'redirect_abort';

// Baseline Self-Assessment (previously pre-survey) - 4 Likert 1-7 items
const BASELINE_QUESTIONS = [
  {
    id: 'baseline_1',
    question: 'I am confident I can recognize when a message asks for sensitive personal information.',
    type: 'likert',
    scale: 7,
    labels: ['Strongly Disagree', '', '', 'Neither Agree nor Disagree', '', '', 'Strongly Agree']
  },
  {
    id: 'baseline_2',
    question: 'I am confident that I can avoid sharing sensitive personal information by accident while messaging.',
    type: 'likert',
    scale: 7,
    labels: ['Strongly Disagree', '', '', 'Neither Agree nor Disagree', '', '', 'Strongly Agree']
  },
  {
    id: 'baseline_3',
    question: 'I am familiar with common online scams and social engineering tactics.',
    type: 'likert',
    scale: 7,
    labels: ['Strongly Disagree', '', '', 'Neither Agree nor Disagree', '', '', 'Strongly Agree']
  },
  {
    id: 'baseline_4',
    question: 'I can tell whether a request for personal information makes sense in the context of a conversation.',
    type: 'likert',
    scale: 7,
    labels: ['Strongly Disagree', '', '', 'Neither Agree nor Disagree', '', '', 'Strongly Agree']
  }
];

// Post-Scenario Survey Questions - Common to both groups (first 4 questions)
const POST_SCENARIO_COMMON_QUESTIONS = [
  {
    id: 'post_scenario_confidence',
    question: 'I felt confident in my judgment about whether to share the requested information.',
    type: 'likert',
    scale: 7,
    labels: ['Strongly Disagree', '', '', 'Neither Agree nor Disagree', '', '', 'Strongly Agree']
  },
  {
    id: 'post_scenario_uncertainty',
    question: 'I felt uncertain or uneasy about sharing information in this conversation.',
    type: 'likert',
    scale: 7,
    labels: ['Strongly Disagree', '', '', 'Neither Agree nor Disagree', '', '', 'Strongly Agree']
  },
  {
    id: 'post_scenario_risk',
    question: 'To what extent did the other person\'s request seem risky?',
    type: 'likert',
    scale: 7,
    labels: ['Not at all risky', '', '', 'Neither low nor high risk', '', '', 'Extremely risky']
  },
  {
    id: 'post_scenario_pii',
    question: 'Which type(s) of personal information did you include in your reply?',
    type: 'checkbox',
    options: [
      'Phone number',
      'Home address',
      'Email address',
      'Date of birth',
      'Workplace or professional affiliation',
      'Government-issued ID number',
      'Financial information (e.g. bank details, salary)',
      'No personal information was disclosed',
      'Other'
    ]
  }
];

// Post-Scenario Survey Questions - Group A only (last 3 questions)
const POST_SCENARIO_A_QUESTIONS = [
  {
    id: 'post_scenario_warning_clarity',
    question: 'The warning was clear.',
    type: 'likert',
    scale: 7,
    labels: ['Strongly Disagree', '', '', 'Neither Agree nor Disagree', '', '', 'Strongly Agree']
  },
  {
    id: 'post_scenario_warning_helpful',
    question: 'The warning helped me notice something new.',
    type: 'likert',
    scale: 7,
    labels: ['Strongly Disagree', '', '', 'Neither Agree nor Disagree', '', '', 'Strongly Agree']
  },
  {
    id: 'post_scenario_rewrite_quality',
    question: 'The suggested rewrite preserved what I wanted to say.',
    type: 'likert',
    scale: 7,
    labels: ['Strongly Disagree', '', '', 'Neither Agree nor Disagree', '', '', 'Strongly Agree']
  }
];

// End-of-Study Survey: SUS questions (Group A only) - 5-point scale
const SUS_QUESTIONS = [
  {
    id: 'sus_1',
    question: 'I think that I would like to use this system frequently.',
    type: 'likert',
    scale: 5,
    labels: ['Strongly Disagree', '', '', '', 'Strongly Agree']
  },
  {
    id: 'sus_2',
    question: 'I found the system unnecessarily complex.',
    type: 'likert',
    scale: 5,
    labels: ['Strongly Disagree', '', '', '', 'Strongly Agree']
  },
  {
    id: 'sus_3',
    question: 'I thought the system was easy to use.',
    type: 'likert',
    scale: 5,
    labels: ['Strongly Disagree', '', '', '', 'Strongly Agree']
  },
  {
    id: 'sus_4',
    question: 'I think that I would need the support of a technical person to be able to use this system.',
    type: 'likert',
    scale: 5,
    labels: ['Strongly Disagree', '', '', '', 'Strongly Agree']
  },
  {
    id: 'sus_5',
    question: 'I found the various functions in this system were well integrated.',
    type: 'likert',
    scale: 5,
    labels: ['Strongly Disagree', '', '', '', 'Strongly Agree']
  },
  {
    id: 'sus_6',
    question: 'I thought there was too much inconsistency in this system.',
    type: 'likert',
    scale: 5,
    labels: ['Strongly Disagree', '', '', '', 'Strongly Agree']
  },
  {
    id: 'sus_7',
    question: 'I would imagine that most people would learn to use this system very quickly.',
    type: 'likert',
    scale: 5,
    labels: ['Strongly Disagree', '', '', '', 'Strongly Agree']
  },
  {
    id: 'sus_8',
    question: 'I found the system very cumbersome to use.',
    type: 'likert',
    scale: 5,
    labels: ['Strongly Disagree', '', '', '', 'Strongly Agree']
  },
  {
    id: 'sus_9',
    question: 'I felt very confident using the system.',
    type: 'likert',
    scale: 5,
    labels: ['Strongly Disagree', '', '', '', 'Strongly Agree']
  },
  {
    id: 'sus_10',
    question: 'I needed to learn a lot of things before I could get going with this system.',
    type: 'likert',
    scale: 5,
    labels: ['Strongly Disagree', '', '', '', 'Strongly Agree']
  }
];

// End-of-Study Survey: Common questions (both groups)
const END_OF_STUDY_COMMON_QUESTIONS = [
  {
    id: 'end_tasks_realistic',
    question: 'Overall, the tasks felt realistic.',
    type: 'likert',
    scale: 7,
    labels: ['Strongly Disagree', '', '', 'Neither Agree nor Disagree', '', '', 'Strongly Agree']
  },
  {
    id: 'end_realism_explanation',
    question: 'What made the study tasks feel realistic or unrealistic?',
    type: 'text'
  },
  {
    id: 'end_overall_confidence',
    question: 'Overall, I felt confident about my decisions on whether to share information.',
    type: 'likert',
    scale: 7,
    labels: ['Strongly Disagree', '', '', 'Neither Agree nor Disagree', '', '', 'Strongly Agree']
  },
  {
    id: 'end_sharing_rationale',
    question: 'Why did you decide to share or not share the requested information? Please refer to any cues, concerns, or assumptions you relied on.',
    type: 'text'
  }
];

// End-of-Study Survey: Group A only questions
const END_OF_STUDY_A_QUESTIONS = [
  {
    id: 'end_trust_system',
    question: 'Overall, I trusted the system\'s warnings and suggestions.',
    type: 'likert',
    scale: 7,
    labels: ['Strongly Disagree', '', '', 'Neither Agree nor Disagree', '', '', 'Strongly Agree']
  },
  {
    id: 'end_trust_explanation',
    question: 'Why did you trust or not trust the system\'s warnings and suggestions?',
    type: 'text'
  }
];

function SurveyScreen({ participantId, participantProlificId, variant }) {
  const navigate = useNavigate();
  const params = useParams();
  const [searchParams] = useSearchParams();
  const surveyType = params.type || 'mid';
  const conversationIndex = searchParams.get('index');
  
  // Determine which questions to show based on survey type and variant
  let questions = [];
  if (surveyType === 'baseline' || surveyType === 'pre') {
    // Baseline Self-Assessment (previously pre-survey)
    questions = BASELINE_QUESTIONS;
  } else if (surveyType === 'mid' || surveyType === 'post-scenario') {
    // Post-Scenario Survey: Common questions + Group A specific questions
    questions = [...POST_SCENARIO_COMMON_QUESTIONS];
    if (variant === 'A') {
      questions = [...questions, ...POST_SCENARIO_A_QUESTIONS];
    }
  } else if (surveyType === 'post' || surveyType === 'end-of-study') {
    // End-of-Study Survey: SUS (Group A only) + Common questions + Group A specific questions
    if (variant === 'A') {
      questions = [...SUS_QUESTIONS, ...END_OF_STUDY_COMMON_QUESTIONS, ...END_OF_STUDY_A_QUESTIONS];
    } else {
      questions = [...END_OF_STUDY_COMMON_QUESTIONS];
    }
  }
  
  const [responses, setResponses] = useState({});
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState(null);
  const [errors, setErrors] = useState({});
  const [otherText, setOtherText] = useState(''); // For "Other" option text input
  const surveyContentRef = useRef(null);
  const questionRefs = useRef({});
  // Reset otherText when survey type or conversation index changes
  useEffect(() => {
    setOtherText('');
    setResponses({});
    setSubmitError(null);
    setErrors({});
    window.scrollTo(0, 0);
    if (surveyContentRef.current) {
      surveyContentRef.current.scrollTop = 0;
    }
  }, [surveyType, conversationIndex]);

  const getScaleLabelRow = (question) => {
    const labels = Array.from({ length: question.scale }, () => '');
    if (question.scale === 7) {
      labels[0] = question.labels?.[0] || 'Strongly Disagree';
      labels[3] = question.labels?.[3] || 'Neither Agree nor Disagree';
      labels[6] = question.labels?.[6] || 'Strongly Agree';
    } else if (question.scale === 5) {
      labels[0] = question.labels?.[0] || 'Strongly Disagree';
      labels[4] = question.labels?.[4] || 'Strongly Agree';
    }
    return labels;
  };

  const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

  const waitForBackend = async () => {
    for (let attempt = 0; attempt < 4; attempt += 1) {
      try {
        await axios.get(`${API_BASE_URL}/health`, { timeout: 3000 });
        return true;
      } catch (error) {
        await sleep(1200);
      }
    }
    return false;
  };

  const postSurvey = async (requestFn, label) => {
    try {
      await requestFn();
      return { ok: true };
    } catch (error) {
      let finalError = error;
      if (!finalError?.response) {
        const recovered = await waitForBackend();
        if (recovered) {
          try {
            await requestFn();
            return { ok: true, retried: true };
          } catch (retryError) {
            finalError = retryError;
          }
        }
      }
      if (finalError?.response?.status === 409) {
        const redirectPath = getRedirectPathFrom409(finalError);
        if (redirectPath) {
          navigate(redirectPath, { replace: true });
          const redirectedError = new Error(REDIRECT_ABORT);
          redirectedError.redirected = true;
          throw redirectedError;
        }
        console.warn(`[Survey] ${label} already submitted`, finalError.response?.data?.detail);
        return { ok: true, conflict: true };
      }
      throw finalError;
    }
  };

  const getSubmitErrorMessage = (error) => {
    if (!error?.response) {
      return 'Backend is unavailable. Please start Docker/backend and try again.';
    }
    const status = error.response.status;
    if (status === 404) {
      return 'Participant record not found. Please refresh and try again.';
    }
    return `Error submitting survey (status ${status}). Please try again.`;
  };

  const handleResponse = (questionId, response) => {
    setResponses({ ...responses, [questionId]: response });
    if (errors[questionId]) {
      const nextErrors = { ...errors };
      delete nextErrors[questionId];
      setErrors(nextErrors);
    }
    // Clear other text if "Other" is deselected in checkbox
    if (questionId === 'post_scenario_pii' && Array.isArray(response) && !response.includes('Other')) {
      setOtherText('');
    }
  };

  const handleCheckboxChange = (questionId, option, checked) => {
    const currentResponse = responses[questionId] || [];
    let newResponse;
    if (checked) {
      newResponse = [...currentResponse, option];
    } else {
      newResponse = currentResponse.filter(item => item !== option);
    }
    setResponses({ ...responses, [questionId]: newResponse });
    if (errors[questionId]) {
      const nextErrors = { ...errors };
      delete nextErrors[questionId];
      setErrors(nextErrors);
    }
    // Clear other text if "Other" is deselected
    if (option === 'Other' && !checked) {
      setOtherText('');
    }
  };

  const handleTextChange = (questionId, value) => {
    setResponses({ ...responses, [questionId]: value });
    if (errors[questionId]) {
      const nextErrors = { ...errors };
      delete nextErrors[questionId];
      setErrors(nextErrors);
    }
  };

  // Check if all questions are answered (with valid non-empty responses)
  const allQuestionsAnswered = () => {
    return questions.every(q => {
      const response = responses[q.id];
      // Text questions: must have non-empty text
      if (q.type === 'text') {
        return response !== undefined && response !== null && response.trim() !== '';
      }
      // Checkbox questions: must have at least one selection
      if (q.type === 'checkbox') {
        if (!Array.isArray(response) || response.length === 0) {
          return false;
        }
        // If "Other" is selected in checkbox, also require the otherText to be filled
        if (q.id === 'post_scenario_pii' && response.includes('Other')) {
          return otherText.trim() !== '';
        }
        return true;
      }
      // Likert/scale questions: must have a value
      return response !== undefined && response !== null && response !== '';
    });
  };

  const validateResponses = () => {
    const nextErrors = {};
    questions.forEach(q => {
      const response = responses[q.id];
      if (q.type === 'checkbox') {
        if (!Array.isArray(response) || response.length === 0) {
          nextErrors[q.id] = 'Please select at least one option.';
          return;
        }
        if (q.id === 'post_scenario_pii' && response.includes('Other') && otherText.trim() === '') {
          nextErrors[q.id] = 'Please specify the "Other" option.';
        }
        return;
      }
      if (q.type === 'text') {
        if (!response || response.trim() === '') {
          nextErrors[q.id] = 'Please provide an answer.';
        }
        return;
      }
      if (q.type === 'likert') {
        if (!response) {
          nextErrors[q.id] = 'Please select a rating.';
        }
      }
    });
    return nextErrors;
  };

  const scrollToFirstError = (nextErrors) => {
    const firstError = questions.find(q => nextErrors[q.id]);
    if (!firstError) return;
    const target = questionRefs.current[firstError.id];
    if (target && typeof target.scrollIntoView === 'function') {
      target.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }
  };

  const handleSubmit = async () => {
    const nextErrors = validateResponses();
    if (Object.keys(nextErrors).length > 0) {
      setErrors(nextErrors);
      scrollToFirstError(nextErrors);
      return;
    }

    setSubmitting(true);
    setSubmitError(null);
    try {
      // Submit to normalized database endpoints based on survey type
      if (surveyType === 'baseline' || surveyType === 'pre') {
        // Baseline Self-Assessment
        await postSurvey(
          () => axios.post(`${API_BASE_URL}/api/participants/${participantId}/baseline-assessment`, {
            recognize_sensitive: parseInt(responses['baseline_1']),
            avoid_accidental: parseInt(responses['baseline_2']),
            familiar_scams: parseInt(responses['baseline_3']),
            contextual_judgment: parseInt(responses['baseline_4'])
          }),
          'Baseline assessment'
        );
      } else if (surveyType === 'mid' || surveyType === 'post-scenario') {
        // Post-Scenario Survey
        const convIndex = parseInt(conversationIndex || '0', 10) + 1; // 1-indexed
        
        // Submit common questions
        await postSurvey(
          () => axios.post(`${API_BASE_URL}/api/participants/${participantId}/post-scenario-survey`, {
            scenario_number: convIndex,
            confidence_judgment: parseInt(responses['post_scenario_confidence']),
            uncertainty_sharing: parseInt(responses['post_scenario_uncertainty']),
            perceived_risk: parseInt(responses['post_scenario_risk']),
            // Group A only fields (nullable)
            warning_clarity: variant === 'A' ? parseInt(responses['post_scenario_warning_clarity']) : null,
            warning_helpful: variant === 'A' ? parseInt(responses['post_scenario_warning_helpful']) : null,
            rewrite_quality: variant === 'A' ? parseInt(responses['post_scenario_rewrite_quality']) : null
          }),
          'Post-scenario survey'
        );
      } else if (surveyType === 'post' || surveyType === 'end-of-study') {
        // End-of-Study Survey
        if (variant === 'A') {
          // Submit SUS responses (Group A only)
          await postSurvey(
            () => axios.post(`${API_BASE_URL}/api/participants/${participantId}/sus-responses`, {
              sus_1: parseInt(responses['sus_1']),
              sus_2: parseInt(responses['sus_2']),
              sus_3: parseInt(responses['sus_3']),
              sus_4: parseInt(responses['sus_4']),
              sus_5: parseInt(responses['sus_5']),
              sus_6: parseInt(responses['sus_6']),
              sus_7: parseInt(responses['sus_7']),
              sus_8: parseInt(responses['sus_8']),
              sus_9: parseInt(responses['sus_9']),
              sus_10: parseInt(responses['sus_10'])
            }),
            'SUS responses'
          );
        }

        // Submit common end-of-study questions
        await postSurvey(
          () => axios.post(`${API_BASE_URL}/api/participants/${participantId}/end-of-study-survey`, {
            tasks_realistic: parseInt(responses['end_tasks_realistic']),
            realism_explanation: responses['end_realism_explanation'] || '',
            overall_confidence: parseInt(responses['end_overall_confidence']),
            sharing_rationale: responses['end_sharing_rationale'] || '',
            // Group A only fields (nullable)
            trust_system: variant === 'A' ? parseInt(responses['end_trust_system']) : null,
            trust_explanation: variant === 'A' ? (responses['end_trust_explanation'] || '') : null
          }),
          'End-of-study survey'
        );
      }

      // Navigate based on survey type
      if (surveyType === 'baseline' || surveyType === 'pre') {
        navigate('/conversation/0', { replace: true });
      } else if (surveyType === 'mid' || surveyType === 'post-scenario') {
        const nextIndex = parseInt(conversationIndex || '0') + 1;
        if (nextIndex < 3) {
          navigate(`/conversation/${nextIndex}`, { replace: true });
        } else {
          // After 3rd conversation post-scenario survey, both groups go to end-of-study
          navigate('/survey/end-of-study', { replace: true });
        }
      } else {
        // End-of-study survey (both groups) -> completion
        navigate('/completion', { replace: true });
      }
    } catch (error) {
      if (error?.redirected || error?.message === REDIRECT_ABORT) {
        return;
      }
      console.error('Error submitting survey:', error);
      const message = getSubmitErrorMessage(error);
      setSubmitError(message);
      alert(message);
    } finally {
      setSubmitting(false);
    }
  };

  const renderQuestion = (question) => {
    if (question.type === 'likert') {
      const labels = getScaleLabelRow(question);
      return (
        <div className="rating-scale">
          <div
            className="rating-track"
            style={{ gridTemplateColumns: `repeat(${question.scale}, 1fr)` }}
          >
            {Array.from({ length: question.scale }, (_, i) => {
              const value = (i + 1).toString();
              return (
                <button
                  key={value}
                  type="button"
                  className={`rating-marker ${responses[question.id] === value ? 'selected' : ''}`}
                  onClick={() => handleResponse(question.id, value)}
                >
                  <span className="marker-dot" />
                </button>
              );
            })}
          </div>
          <div
            className="rating-labels"
            style={{ gridTemplateColumns: `repeat(${question.scale}, 1fr)` }}
          >
            {labels.map((label, idx) => (
              <div key={`${question.id}-label-${idx}`} className="rating-label">
                {label}
              </div>
            ))}
          </div>
        </div>
      );
    } else if (question.type === 'checkbox') {
      const selectedOptions = responses[question.id] || [];
      const showOtherInput = question.id === 'post_scenario_pii' && selectedOptions.includes('Other');
      return (
        <div className="multiple-choice-options">
          {question.options.map((option, idx) => (
            <label key={idx} className="multiple-choice-option">
              <input
                type="checkbox"
                name={question.id}
                value={option}
                checked={selectedOptions.includes(option)}
                onChange={(e) => handleCheckboxChange(question.id, option, e.target.checked)}
              />
              <span>{option}</span>
            </label>
          ))}
          {showOtherInput && (
            <div className="other-input-container" style={{ marginTop: '12px', marginLeft: '24px' }}>
              <input
                type="text"
                placeholder="Please specify..."
                value={otherText}
                onChange={(e) => {
                  const value = e.target.value;
                  setOtherText(value);
                  if (errors['post_scenario_pii'] && value.trim() !== '') {
                    const nextErrors = { ...errors };
                    delete nextErrors['post_scenario_pii'];
                    setErrors(nextErrors);
                  }
                }}
                className="other-text-input"
                style={{
                  width: '100%',
                  maxWidth: '400px',
                  padding: '8px 12px',
                  border: '1px solid #ccc',
                  borderRadius: '4px',
                  fontSize: '14px'
                }}
              />
            </div>
          )}
        </div>
      );
    } else if (question.type === 'text') {
      return (
        <div className="text-input-container">
          <textarea
            value={responses[question.id] || ''}
            onChange={(e) => handleTextChange(question.id, e.target.value)}
            placeholder="Please provide your answer..."
            className="text-input"
            style={{
              width: '100%',
              minHeight: '100px',
              padding: '12px',
              border: '1px solid #ccc',
              borderRadius: '4px',
              fontSize: '14px',
              fontFamily: 'inherit',
              resize: 'vertical'
            }}
          />
        </div>
      );
    }
    return null;
  };

  return (
    <div className="survey-screen">
      <div className="survey-content" ref={surveyContentRef}>
        <h1>Survey Questions</h1>
        <p>Please answer the following questions about your experience.</p>
        
        <div className="survey-questions">
          {questions.map((question) => (
            <div
              key={question.id}
              className="survey-question"
              ref={(el) => {
                questionRefs.current[question.id] = el;
              }}
            >
              {errors[question.id] && (
                <div className="question-error">{errors[question.id]}</div>
              )}
              <label className="question-label">{question.question}</label>
              {renderQuestion(question)}
            </div>
          ))}
        </div>

        {submitError && (
          <div className="submit-error">
            {submitError}
          </div>
        )}
        
        <button
          className="submit-button"
          onClick={handleSubmit}
          disabled={submitting || !allQuestionsAnswered()}
        >
          {submitting ? 'Submitting...' : 'Continue'}
        </button>
      </div>
    </div>
  );
}

export default SurveyScreen;
