import { handleActions } from "redux-actions";

import { authActions } from "./actions";

const token = localStorage.getItem("token");

const initialState = {
  isAuthenticated: !!token || false
};

export const authReducer = handleActions({}, initialState);
