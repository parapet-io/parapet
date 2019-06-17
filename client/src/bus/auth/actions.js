import { createActions } from "redux-actions";

export const authActions = createActions({
  //Sync

  //Async
  LOGIN_ASYNC: credentials => credentials,
  REGISTRATION_ASYNC: credentials => credentials
});
