import { put, apply } from "redux-saga/effects";

import { api } from "REST";

import { authActions } from "bus/auth/actions";

export function* registration({ payload: credentials }) {
  try {
    console.log(credentials);
    const responce = yield apply(api, api.auth.registration, [credentials]);
    const { data, message } = yield apply(responce, responce.json);

    // yield apply(localStorage, localStorage.setItem, ["token", data.token]);

    if (responce.status !== 200) {
      throw new Error(message);
    }
  } catch (error) {
    console.error(error);
  } finally {
  }
}
