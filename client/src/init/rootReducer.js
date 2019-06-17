import { combineReducers } from "redux";

import { authReducer as auth } from "bus/auth/reducer";
import { componentReducer as component } from "bus/component/reducer";

export const rootReducer = combineReducers({ auth, component });
