import { createActions } from "redux-actions";

export const authActions = createActions({
  //Sync
  AUTHENTICATE: credentials => credentials,
  //Async
  LOGIN_ASYNC: credentials => credentials,
  REGISTRATION_ASYNC: credentials => credentials,
  AUTHENTICATE_ASYNC: id => id
});
