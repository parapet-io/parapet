import { MAIN_URL } from "../config";

export default class Auth {
  static get token() {
    return localStorage.getItem("_id");
  }

  registration(credentials) {
    return fetch(`${MAIN_URL}/v1/users`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(credentials)
    });
  }
  login(credentials) {
    return fetch(`${MAIN_URL}/v1/login`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(credentials)
    });
  }
  getUser(id) {
    return (
      !!Auth.token &&
      fetch(`${MAIN_URL}/v1/users/${id}`, {
        method: "GET"
      })
    );
  }
}
