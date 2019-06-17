import React from "react";

import HeaderMenu from "../Menus/HeaderMenu/HeaderMenu";

import s from "./Header.module.scss";

const Header = () => {
  return (
    <div className={s.home}>
      <HeaderMenu />
      <div className={s.headerMain}>
        <span>Parapet.io</span>
        <span>Build your distributed system</span>
      </div>
    </div>
  );
};

export default Header;
