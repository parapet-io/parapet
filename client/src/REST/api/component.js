import { MAIN_URL } from "../config";

export default class Component {
  static get token() {
    return localStorage.getItem("_id");
  }

  registerComponent(credentials) {
    return fetch(`${MAIN_URL}/v1/components`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(credentials)
    });
  }
  getComponentsByUser(id) {
    return fetch(`${MAIN_URL}/v1/components/getByUserId/${id}`, {
      method: "GET"
    });
  }
}
