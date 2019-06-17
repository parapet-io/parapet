import { handleActions } from "redux-actions";

import { authActions } from "./actions";

const token = localStorage.getItem("_id");

const initialState = {
  isAuthenticated: !!token || false
};

export const authReducer = handleActions(
  {
    [authActions.authenticate]: state => {
      return {
        ...state,
        isAuthenticated: true
      };
    },
    [authActions.logout]: state => {
      return {
        ...state,
        isAuthenticated: false
      };
    }
  },
  initialState
);
