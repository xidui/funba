/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./app/**/*.{ts,tsx}", "./components/**/*.{ts,tsx}"],
  presets: [require("nativewind/preset")],
  theme: {
    extend: {
      colors: {
        bg: "#080f1a",
        surface: "#0d1c2e",
        surface2: "#14253a",
        border: "#1f3048",
        text: "#e6ebf2",
        muted: "#7d8a9c",
        accent: "#f97316",
        "accent-soft": "#fbbf24",
        win: "#4ade80",
        loss: "#f87171",
      },
      fontFamily: {
        head: ["Outfit"],
        sans: ["Inter"],
      },
    },
  },
  plugins: [],
};
