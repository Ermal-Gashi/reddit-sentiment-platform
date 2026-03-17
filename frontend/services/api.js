import axios from 'axios';

// ----------------------------------------------------------------------
// CONFIGURATION
// ----------------------------------------------------------------------
// Ensure this matches your Python Backend URL
const API_BASE_URL = 'http://127.0.0.1:8000';

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 10000, // 10 seconds matches your old config
  headers: {
    'Content-Type': 'application/json',
  },
});

// ----------------------------------------------------------------------
// REQUEST INTERCEPTOR (Your old logging logic)
// ----------------------------------------------------------------------
api.interceptors.request.use((config) => {
  // Helpful for seeing what gets sent to the backend
  console.log("📤 Request:", config.url, config.params || config.data);
  return config;
});

// ----------------------------------------------------------------------
// RESPONSE INTERCEPTOR (Global Error Handling)
// ----------------------------------------------------------------------
api.interceptors.response.use(
  (response) => response,
  (error) => {
    // Detailed error logging
    if (error.response) {
      // Server responded with a status code (4xx, 5xx)
      console.error('🔴 Backend Error:', error.response.status, error.response.data);
    } else if (error.request) {
      // Request was made but no response received
      console.error('🔴 Network Error: No response from backend. Is it running?');
    } else {
      // Something happened in setting up the request
      console.error('🔴 Request Error:', error.message);
    }
    return Promise.reject(error);
  }
);

export default api;
