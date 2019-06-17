import { takeEvery, all, call } from "redux-saga/effects";

import { componentActions } from "../actions";

import { registerComponent, getComponentsByUser } from "./workers";

export function* watchRegisterComponent() {
  yield takeEvery(componentActions.registerComponentAsync, registerComponent);
}

export function* watchGetComponetsByUser() {
  yield takeEvery(
    componentActions.getComponentsByUserAsync,
    getComponentsByUser
  );
}
export default function* watchComponent() {
  yield all([call(watchRegisterComponent), call(watchGetComponetsByUser)]);
}
