import { takeEvery, all, call } from "redux-saga/effects";

import { typeActions } from "../actions";

import { worker } from "./workers";

export function* watchWorker() {
  yield takeEvery(typeActions.typeDefault, worker);
}

export function* watchDomain() {
  yield all([call(watchWorker)]);
}
