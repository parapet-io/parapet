import { combineReducers } from "redux";

import { authReducer as auth } from "bus/auth/reducer";

export const rootReducer = combineReducers({ auth });
