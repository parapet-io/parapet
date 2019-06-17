import React, { useState } from "react";
import { Link } from "react-router-dom";

import LoginModal from "../../Modals/LoginModal";
import RegistrationModal from "../../Modals/RegistrationModal";

import s from "./HeaderMenu.module.scss";

const HeaderMenu = () => {
  const [isLoginOpen, setIsLoginOpen] = useState(false);
  const [isRegistrationOpen, setIsRegistrationOpen] = useState(false);

  const handleLoginClose = () => setIsLoginOpen(false);
  const handleLoginOpen = () => setIsLoginOpen(true);

  const handleRegistrationClose = () => setIsRegistrationOpen(false);
  const handleRegistrationOpen = () => setIsRegistrationOpen(true);

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
        <button
          className={s.headerRightBtn}
          type="button"
          onClick={handleLoginOpen}
        >
          Login
        </button>
        <button
          className={s.headerRightBtn}
          type="button"
          onClick={handleRegistrationOpen}
        >
          Register
        </button>
      </div>
      <LoginModal open={isLoginOpen} handleClose={handleLoginClose} />
      <RegistrationModal
        open={isRegistrationOpen}
        handleClose={handleRegistrationClose}
      />
    </div>
  );
};

export default HeaderMenu;
