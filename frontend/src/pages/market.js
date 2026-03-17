import React from 'react';
import Head from 'next/head';
import MarketView from '../components/domain/market/MarketView';

export default function MarketPage(props) {
  // props contains { marketState, updateMarketState } passed from Layout.js
  return (
    <>
      <Head>
        <title>Market Analysis | Thesis.io</title>
      </Head>

      {/* Pass the Layout state down to the View */}
      <MarketView {...props} />
    </>
  );
}