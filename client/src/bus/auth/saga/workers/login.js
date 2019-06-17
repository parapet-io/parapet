import { put, apply } from "redux-saga/effects";

import { api } from "REST";

import { authActions } from "bus/auth/actions";

export function* login({ payload: credentials }) {
  try {
    const responce = yield apply(api, api.auth.login, [credentials]);
    const { data, message } = yield apply(responce, responce.json);

    if (responce.status !== 200) {
      throw new Error(message);
    }

    yield apply(localStorage, localStorage.setItem, ["token", data.token]);
  } catch (error) {
    console.error(error);
  }
}
