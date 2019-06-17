import { put, apply } from "redux-saga/effects";

import { api } from "REST";

import { authActions } from "bus/auth/actions";

export function* login({ payload: credentials }) {
  try {
    const responce = yield apply(api, api.auth.login, [credentials]);
    const { _id, email, registrationDate } = yield apply(
      responce,
      responce.json
    );

    if (responce.status !== 200) {
      throw new Error("error");
    }

    console.log(_id);

    yield apply(localStorage, localStorage.setItem, ["_id", _id]);
    yield put(authActions.authenticate());
  } catch (error) {
    console.error(error);
  }
}
