import React, { useState } from "react";
import { connect } from "react-redux";

import { authActions } from "bus/auth/actions";

import Dialog from "@material-ui/core/Dialog";
import DialogActions from "@material-ui/core/DialogActions";
import DialogContent from "@material-ui/core/DialogContent";
import Button from "@material-ui/core/Button";
import DialogTitle from "@material-ui/core/DialogTitle";
import TextField from "@material-ui/core/TextField";

const LoginModal = ({ open, handleClose, loginAsync }) => {
  const [values, setValues] = useState({
    email: "",
    password: ""
  });

  const handleChange = name => event => {
    setValues({ ...values, [name]: event.target.value });
  };

  const clearForm = () =>
    setValues({
      email: "",
      password: ""
    });

  const validateForm = () =>
    values.email && values.password && values.password.length > 5;

  const handleCloseForm = () => clearForm() || handleClose();

  const handleSubmit = () => {
    if (validateForm()) {
      const credentials = {
        email: values.email,
        password: values.password
      };
      loginAsync(credentials);
      handleCloseForm();
    }
  };

  return (
    <Dialog open={open} onClose={handleCloseForm}>
      <DialogTitle id="form-dialog-title">Login</DialogTitle>
      <DialogContent>
        <div className="modalForm">
          <TextField
            label="Email"
            value={values.name}
            onChange={handleChange("email")}
            margin="normal"
            variant="outlined"
          />
          <TextField
            label="Password"
            type="password"
            value={values.password}
            onChange={handleChange("password")}
            margin="normal"
            variant="outlined"
          />
        </div>
      </DialogContent>
      <DialogActions>
        <Button onClick={handleCloseForm} color="primary">
          Cancel
        </Button>
        <Button onClick={handleSubmit} color="primary">
          Login
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default connect(
  null,
  { loginAsync: authActions.loginAsync }
)(LoginModal);
