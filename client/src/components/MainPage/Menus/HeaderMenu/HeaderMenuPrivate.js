import React, { useState } from "react";
import { Link } from "react-router-dom";

import s from "./HeaderMenu.module.scss";

const HeaderMenuPrivate = () => {
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
        <Link
          to="/managment-console"
          className={s.headerRightLink}
          type="button"
        >
          Managment Console
        </Link>
      </div>
    </div>
  );
};

export default HeaderMenuPrivate;
