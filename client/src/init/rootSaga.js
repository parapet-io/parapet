import { all, call } from "redux-saga/effects";

import watchAuth from "bus/auth/saga/watchers";
import watchComponent from "bus/component/saga/watchers";

export function* rootSaga() {
  yield all([call(watchAuth), call(watchComponent)]);
}
