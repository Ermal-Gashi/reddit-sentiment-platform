import '../styles/globals.css'; // Import the Tailwind styles
import Layout from '../components/layout/Layout';

function MyApp({ Component, pageProps }) {
  return (
    // Wrap every page in our Dashboard Layout
    <Layout>
      <Component {...pageProps} />
    </Layout>
  );
}

export default MyApp;



