import { put, apply } from "redux-saga/effects";

import { componentActions } from "../../actions";

import { api } from "REST";

export function* getComponentsByUser({ payload: id }) {
  try {
    const responce = yield apply(api, api.component.getComponentsByUser, [id]);
    const data = yield apply(responce, responce.json);
    yield put(componentActions.setComponents(data));

    if (responce.status !== 200) {
      // throw new Error(message);
      throw new Error("error");
    }
  } catch (error) {
    console.error(error);
  }
}
