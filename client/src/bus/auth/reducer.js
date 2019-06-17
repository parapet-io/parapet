import { handleActions } from "redux-actions";

import { authActions } from "./actions";

const token = localStorage.getItem("_id");

const initialState = {
  isAuthenticated: !!token || false,
  user: {}
};

export const authReducer = handleActions(
  {
    [authActions.authenticate]: (state, { payload }) => {
      return {
        ...state,
        isAuthenticated: true,
        user: { ...payload }
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
