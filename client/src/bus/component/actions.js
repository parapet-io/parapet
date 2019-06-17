import { createActions } from "redux-actions";

export const componentActions = createActions({
  //Sync
  SET_COMPONENT: component => component,
  SET_COMPONENTS: components => components,
  //Async
  REGISTER_COMPONENT_ASYNC: credentials => credentials,
  GET_COMPONENTS_BY_USER_ASYNC: id => id
});
