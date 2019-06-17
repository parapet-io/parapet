import { MAIN_URL } from "../config";

export default class Auth {
  static get token() {
    return localStorage.getItem("token");
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
}