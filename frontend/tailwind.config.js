/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        ink: '#111827',
        canvas: '#f5f7fb',
        brand: '#5b6df8',
      },
    },
  },
  plugins: [],
}
