import React from "react";
import { Switch, Route } from "react-router-dom";
import { HeaderMenu, ManagmentConsole } from "components";
import { MainPage } from "pages";

function App() {
  return (
    <div className="App">
      <HeaderMenu />
      <Switch>
        <Route exact path="/" component={MainPage} />
        <Route path="/managment-console" component={ManagmentConsole} />
      </Switch>
    </div>
  );
}

export default App;
