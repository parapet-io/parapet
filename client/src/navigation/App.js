import React, { useEffect } from "react";
import { connect } from "react-redux";
import { Switch, Route } from "react-router-dom";

import { authActions } from "bus/auth/actions";

import { HeaderMenu, ManagmentConsole } from "components";
import { MainPage } from "pages";

const App = ({ authenticate }) => {
  useEffect(() => {
    const id = localStorage.getItem("_id");
    if (id) {
      authenticate(id);
    }
  }, []);
  return (
    <div className="App">
      <HeaderMenu />
      <Switch>
        <Route exact path="/" component={MainPage} />
        <Route path="/managment-console" component={ManagmentConsole} />
      </Switch>
    </div>
  );
};
export default connect(
  null,
  { authenticate: authActions.authenticateAsync }
)(App);
