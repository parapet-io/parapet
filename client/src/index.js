import React from "react";
import { render } from "react-dom";
import { BrowserRouter as Router } from "react-router-dom";
import { Provider } from "react-redux";

import { store } from "./init/store";

import App from "./navigation/App";

import "./assets/global.scss";

render(
  <Provider store={store}>
    <Router>
      <App />
    </Router>
  </Provider>,
  document.getElementById("root")
);
