import Auth from "./auth";

class Api {
  auth = new Auth();
}

export const api = new Api();
