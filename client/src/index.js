import React from "react";
import { render } from "react-dom";
import { BrowserRouter as Router } from "react-router-dom";

import "./assets/global.scss";

import App from "./navigation/App";

render(
  <Router>
    <App />
  </Router>,
  document.getElementById("root")
);
