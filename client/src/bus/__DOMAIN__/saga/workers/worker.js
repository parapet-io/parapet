import { put, apply } from 'redux-saga/effects';

import { api } from 'REST';

import { uiActions } from 'bus/ui/actions';

export function* worker() {
  try {
    yield put(uiActions.startFetching());

    const responce = yield apply(api, api.posts.fetch);
    const { data: posts, message } = yield apply(responce, responce.json);

    if (responce.status !== 200) {
      throw new Error(message);
    }
  } catch (error) {
    yield put(uiActions.emitError(error, 'createPost worker'));
  } finally {
    yield put(uiActions.stopFetching());
  }
}
