import React, { useEffect, useRef } from 'react';
import MessageBubble from './MessageBubble';
import DateSeparator from './DateSeparator';
import './MessageList.css';

function MessageList({ messages, conversationKey }) {
  const listRef = useRef(null);
  const wasNearBottomRef = useRef(true);
  const skipAutoScrollRef = useRef(true);

  useEffect(() => {
    if (listRef.current) {
      listRef.current.scrollTop = 0;
    }
    skipAutoScrollRef.current = true;
    wasNearBottomRef.current = false;
  }, [conversationKey]);

  useEffect(() => {
    if (skipAutoScrollRef.current && listRef.current) {
      listRef.current.scrollTop = 0;
      skipAutoScrollRef.current = false;
      return;
    }
    if (listRef.current && wasNearBottomRef.current) {
      listRef.current.scrollTop = listRef.current.scrollHeight;
    }
  }, [messages]);

  const handleScroll = () => {
    if (!listRef.current) return;
    const { scrollTop, scrollHeight, clientHeight } = listRef.current;
    const distanceFromBottom = scrollHeight - scrollTop - clientHeight;
    wasNearBottomRef.current = distanceFromBottom < 60;
  };

  const formatDate = (date) => {
    const today = new Date();
    const messageDate = new Date(date);
    const diffTime = Math.abs(today - messageDate);
    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));

    if (diffDays === 1) {
      return 'Today';
    } else if (diffDays === 2) {
      return 'Yesterday';
    } else {
      return messageDate.toLocaleDateString();
    }
  };

  const needsDateSeparator = (prevDate, currentDate) => {
    if (!prevDate) return true;
    const prev = new Date(prevDate);
    const curr = new Date(currentDate);
    return prev.toDateString() !== curr.toDateString();
  };

  return (
    <div className="message-list" ref={listRef} onScroll={handleScroll}>
      {messages.map((message, index) => {
        const prevMessage = index > 0 ? messages[index - 1] : null;
        const showSeparator = needsDateSeparator(
          prevMessage?.timestamp,
          message.timestamp
        );

        return (
          <React.Fragment key={message.id}>
            {showSeparator && (
              <DateSeparator date={formatDate(message.timestamp)} />
            )}
            <MessageBubble message={message} />
          </React.Fragment>
        );
      })}
    </div>
  );
}

export default MessageList;
