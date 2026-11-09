import React from "react";
import ReactDOM from "react-dom/client";
import AuthBoundary from "./AuthBoundary";
import { restoreTheme } from "./ThemePicker";
import "./styles.css";
import "./themes.css";

restoreTheme();
ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <AuthBoundary />
  </React.StrictMode>,
);
