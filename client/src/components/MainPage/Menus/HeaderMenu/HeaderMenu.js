import React from "react";

import HeaderMenuPublic from "./HeaderMenuPublic";
import HeaderMenuPrivate from "./HeaderMenuPrivate";

const HeaderMenu = () => {
  return <>{true ? <HeaderMenuPrivate /> : <HeaderMenuPublic />}</>;
};

export default HeaderMenu;
