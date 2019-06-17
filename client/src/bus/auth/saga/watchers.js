import { takeEvery, all, call } from "redux-saga/effects";

import { authActions } from "../actions";

import { registration, login, authenticate } from "./workers";

export function* watchRegistration() {
  yield takeEvery(authActions.registrationAsync, registration);
}

export function* watchLogin() {
  yield takeEvery(authActions.loginAsync, login);
}

export function* watchAuthenticate() {
  yield takeEvery(authActions.authenticateAsync, authenticate);
}

export default function* watchAuth() {
  yield all([
    call(watchRegistration),
    call(watchLogin),
    call(watchAuthenticate)
  ]);
}
