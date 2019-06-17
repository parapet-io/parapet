import { put, apply } from "redux-saga/effects";

import { componentActions } from "../../actions";

import { api } from "REST";

export function* registerComponent({ payload: credentials }) {
  try {
    const responce = yield apply(api, api.component.registerComponent, [
      credentials
    ]);
    const data = yield apply(responce, responce.json);

    yield put(componentActions.setComponent(data));

    if (responce.status !== 200) {
      // throw new Error(message);
      throw new Error("error");
    }
  } catch (error) {
    console.error(error);
  }
}
