import { applyMiddleware, compose } from "redux";
import { createLogger } from "redux-logger";
import createSagaMiddleware from "redux-saga";

const loggerMiddleware = createLogger({
  duration: true,
  collapsed: true,
  colors: {
    title: () => "#139BFE",
    prevState: () => "#1c5faf",
    action: () => "#149945",
    nextState: () => "#A47104",
    error: () => "#ff0005"
  }
});

const sagaMiddleware = createSagaMiddleware();
const devtools = window.__REDUX_DEVTOOLS_EXTENSION_COMPOSE__;
const composeEnchancers = devtools ? devtools : compose;

const enhancedStore = composeEnchancers(
  applyMiddleware(sagaMiddleware, loggerMiddleware)
);

export { enhancedStore, sagaMiddleware };
