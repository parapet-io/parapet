import { handleActions } from "redux-actions";

import { typeActions } from "./actions";

const initialState = {
  data: {}
};

export const tasksReducer = handleActions(
  {
    [typeActions.typeDefault]: (state, { payload }) => {
      return {
        ...state,
        data: payload
      };
    }
  },
  initialState
);
