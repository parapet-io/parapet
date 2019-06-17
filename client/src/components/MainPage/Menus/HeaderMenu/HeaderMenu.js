import React from "react";
import { connect } from "react-redux";

import HeaderMenuPublic from "./HeaderMenuPublic";
import HeaderMenuPrivate from "./HeaderMenuPrivate";
import { isAuthenticated } from "bus/auth/selectors";

const HeaderMenu = ({ isAuthenticated }) => {
  return <>{isAuthenticated ? <HeaderMenuPrivate /> : <HeaderMenuPublic />}</>;
};

const MSTP = state => ({
  isAuthenticated: isAuthenticated(state)
});

export default connect(MSTP)(HeaderMenu);
