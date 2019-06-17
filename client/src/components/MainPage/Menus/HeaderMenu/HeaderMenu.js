import React from "react";

import HeaderMenuPublic from "./HeaderMenuPublic";
import HeaderMenuPrivate from "./HeaderMenuPrivate";

const HeaderMenu = () => {
  return <>{false ? <HeaderMenuPrivate /> : <HeaderMenuPublic />}</>;
};

export default HeaderMenu;
