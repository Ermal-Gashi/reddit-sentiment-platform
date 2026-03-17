import React from 'react';
import { useTopicViewData } from '../../../hooks/useTopicViewData';

// Modules
import TopicGrid from './Modules/TopicGrid';
import TopicDetails from './Modules/TopicDetails';

export default function TopicView(props) {
  const {
    loading,
    error,
    dateRange,
    topics,
    selectedTopic,
    setSelectedTopicId,

    // Representative sentences
    topicRepresentatives,
  } = useTopicViewData(props);

  return (
    <div className="h-full animate-in fade-in duration-500">
      {error && (
        <div className="mb-4 bg-rose-500/10 border border-rose-500/40 text-rose-200 px-4 py-3 rounded-lg text-sm font-mono flex items-center gap-3">
          <div className="w-2 h-2 rounded-full bg-rose-500 animate-pulse"></div>
          TOPIC API ERROR: {error}
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-[360px_1fr] gap-6 h-full">
        {/* LEFT: Topic List */}
        <div className="h-full">
          <TopicGrid
            topics={topics}
            selectedTopicId={selectedTopic?.topic_id}
            onSelectTopic={setSelectedTopicId}
          />
        </div>

        {/* RIGHT: Topic Details */}
        <div className="h-full overflow-y-auto pr-1">
          {/* topics is required for TopicDistribution */}
          <TopicDetails
            topic={selectedTopic}
            topics={topics}
            dateRange={dateRange}
            loading={loading}
            topicRepresentatives={topicRepresentatives}
          />
        </div>
      </div>
    </div>
  );
}
