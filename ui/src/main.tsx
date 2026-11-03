import React from "react";
import ReactDOM from "react-dom/client";
import AuthBoundary from "./AuthBoundary";
import "./styles.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <AuthBoundary />
  </React.StrictMode>,
);
