import React from 'react';
import Head from 'next/head';
import TopicView from '../components/domain/topics/TopicView';

export default function TopicsPage(props) {
  // props contains { topicState, updateTopicState } passed from Layout.js
  return (
    <>
      <Head>
        <title>Topic Modeling | Thesis.io</title>
      </Head>

      {/* Pass the Layout state down to the View */}
      <TopicView {...props} />
    </>
  );
}
