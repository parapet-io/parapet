import React from "react";

import { Link } from "react-router-dom";

import s from "./UnderHeaderMenu.module.scss";

const UnderHeaderMenu = () => {
  return (
    <div className={s.underHeaderWrapper}>
      <Link to="/">Data Types</Link>
      <Link to="/">Tutorial</Link>
      <Link to="/">Concurrency Basis</Link>
      <Link to="/">Concurrency</Link>
      <Link to="/">Type Classes</Link>
    </div>
  );
};

export default UnderHeaderMenu;
