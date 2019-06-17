import { handleActions } from "redux-actions";

import { componentActions } from "./actions";

const token = localStorage.getItem("_id");

const initialState = {
  components: [],
  component: {}
};

export const componentReducer = handleActions(
  {
    [componentActions.setComponent]: (state, { payload }) => {
      return {
        ...state,
        components: [...state.components, payload],
        component: payload
      };
    },
    [componentActions.setComponents]: (state, { payload }) => {
      console.log(payload);
      return {
        ...state,
        components: payload
      };
    }
  },
  initialState
);
