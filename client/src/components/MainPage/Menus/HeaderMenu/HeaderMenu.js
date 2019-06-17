import React from "react";
import { Link } from "react-router-dom";

import s from "./HeaderMenu.module.scss";

const HeaderMenu = () => {
  return (
    <div className={s.headerWrapper}>
      <div className={s.headerLeft}>
        <Link className={s.logo} to="/">
          Parapet>
        </Link>
      </div>

      <div className={s.headerRight}>
        <a
          className={s.headerRightLink}
          href="/"
          target="_blank"
          rel="noopener noreferrer"
        >
          Github
        </a>
        <a
          className={s.headerRightLink}
          href="/"
          target="_blank"
          rel="noopener noreferrer"
        >
          Documentation
        </a>
        <button className={s.headerRightBtn} type="button">
          Login
        </button>
        <button className={s.headerRightBtn} type="button">
          Register
        </button>
      </div>
    </div>
  );
};

export default HeaderMenu;
