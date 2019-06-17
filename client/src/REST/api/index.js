import Auth from "./auth";
import Component from "./component";

class Api {
  auth = new Auth();
  component = new Component();
}

export const api = new Api();
