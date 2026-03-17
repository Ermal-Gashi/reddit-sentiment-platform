/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{js,ts,jsx,tsx}",
    "./pages/**/*.{js,ts,jsx,tsx}",
    "./components/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // 1. THE CHARCOAL THEME (Deep Dark Mode)
        navy: {
          900: '#09090b', // Zinc-950
          800: '#18181b', // Zinc-900
          700: '#27272a', // Zinc-800
        },

        // 2. REDDIT ORANGE ACCENTS (mapped to emerald classes)
        emerald: {
          50: '#fff7ed',
          100: '#ffedd5',
          200: '#fed7aa',
          300: '#fdba74',
          400: '#fb923c',
          500: '#f97316', // Main orange
          600: '#ea580c',
          700: '#c2410c',
          800: '#9a3412',
          900: '#7c2d12',
          950: '#431407',
        },

        // 3. SECRET GREEN (mapped to brown)
        brown: {
          50: '#ecfdf5',
          100: '#d1fae5',
          200: '#a7f3d0',
          300: '#6ee7b7',
          400: '#34d399',
          500: '#10b981',
          600: '#059669',
          700: '#047857',
          800: '#065f46',
          900: '#064e3b',
        },
      },

      fontFamily: {
        sans: ['Inter', 'sans-serif'],
      },

      cursor: {
        emerald:
          "url(\"data:image/svg+xml,%3Csvg width='32' height='32' viewBox='0 0 32 32' fill='none' xmlns='http://www.w3.org/2000/svg'%3E%3Cpath d='M6 2L26 14L16 16L14 26L6 2Z' fill='%23f97316' stroke='white' stroke-width='2' stroke-linejoin='round'/%3E%3C/svg%3E\") 4 4, auto",
      },
    },
  },
  plugins: [],
};
