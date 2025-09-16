// eslint.config.js
import js from "@eslint/js";
import reactHooks from "eslint-plugin-react-hooks";
import react from "eslint-plugin-react";
import reactRefresh from "eslint-plugin-react-refresh";

export default [
  js.configs.recommended,
  // react-hooks 5.x：已提供 flat config 推薦組合
  reactHooks.configs["recommended-latest"],
  {
    files: ["**/*.{js,jsx}"],
    plugins: {
      react,
      "react-refresh": reactRefresh
    },
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module"
    },
    settings: {
      react: { version: "detect" }
    },
    rules: {
      // Vite + React 常見需求
      "react/jsx-uses-react": "off",
      "react/react-in-jsx-scope": "off",
      // Vite HMR 專用規則（選用，但很實用）
      "react-refresh/only-export-components": ["warn", { allowConstantExport: true }]
    }
  }
];
