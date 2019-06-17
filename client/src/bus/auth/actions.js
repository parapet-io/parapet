import { createActions } from "redux-actions";

export const authActions = createActions({
  //Sync
  AUTHENTICATE: void 0,
  //Async
  LOGIN_ASYNC: credentials => credentials,
  REGISTRATION_ASYNC: credentials => credentials
});
