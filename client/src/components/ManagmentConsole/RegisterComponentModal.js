import React, { useState } from "react";
import { connect } from "react-redux";

import { authActions } from "bus/auth/actions";

import { makeStyles } from "@material-ui/core/styles";
import Dialog from "@material-ui/core/Dialog";
import DialogActions from "@material-ui/core/DialogActions";
import DialogContent from "@material-ui/core/DialogContent";
import Button from "@material-ui/core/Button";
import DialogTitle from "@material-ui/core/DialogTitle";
import TextField from "@material-ui/core/TextField";
import InputLabel from "@material-ui/core/InputLabel";
import MenuItem from "@material-ui/core/MenuItem";

import FormControl from "@material-ui/core/FormControl";
import Select from "@material-ui/core/Select";

const RegisterComponentModal = ({ open, handleClose }) => {
  const [values, setValues] = useState({
    name: "",
    decsription: "",
    author: "",
    license: "",
    repoUrl: "",
    homepage: "",
    tags: ""
  });

  const useStyles = makeStyles(theme => ({
    root: {
      display: "flex",
      flexWrap: "wrap"
    },
    formControl: {
      margin: theme.spacing(1),
      minWidth: 120
    },
    selectEmpty: {
      marginTop: theme.spacing(2)
    }
  }));
  const handleChange = name => event => {
    setValues({ ...values, [name]: event.target.value });
  };

  const clearForm = () =>
    setValues({
      name: "",
      decsription: "",
      author: "",
      license: "",
      repoUrl: "",
      homepage: "",
      tags: ""
    });

  const validateForm = () =>
    values.email && values.password && values.password.length > 5;

  const handleCloseForm = () => clearForm() || handleClose();

  const classes = useStyles();
  return (
    <Dialog open={open} onClose={handleCloseForm}>
      <DialogTitle id="form-dialog-title">Component registration</DialogTitle>
      <DialogContent>
        <div className="modalForm">
          <TextField
            label="Name"
            value={values.name}
            onChange={handleChange("name")}
            margin="normal"
            variant="outlined"
          />
          <TextField
            label="Description"
            value={values.description}
            onChange={handleChange("description")}
            margin="normal"
            variant="outlined"
          />
          <TextField
            label="Author"
            value={values.author}
            onChange={handleChange("author")}
            margin="normal"
            variant="outlined"
          />
          <FormControl className={classes.formControl}>
            <InputLabel htmlFor="license-simple">License</InputLabel>
            <Select
              value={values.license}
              onChange={handleChange("license")}
              inputProps={{
                name: "license",
                id: "license-simple"
              }}
            >
              <MenuItem value="">
                <em>None</em>
              </MenuItem>
              <MenuItem value={"MIT"}>MIT</MenuItem>
              <MenuItem value={"ISC"}>ISC</MenuItem>
            </Select>
          </FormControl>
          <TextField
            label="Repository URL"
            value={values.repoUrl}
            onChange={handleChange("repoUrl")}
            margin="normal"
            variant="outlined"
          />
          <TextField
            label="Homepage"
            value={values.homepage}
            onChange={handleChange("homepage")}
            margin="normal"
            variant="outlined"
          />
        </div>
      </DialogContent>
      <DialogActions>
        <Button onClick={handleCloseForm} color="primary">
          Cancel
        </Button>
        <Button onClick={() => console.log("submit")} color="primary">
          Register component
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default connect(
  null,
  { loginAsync: authActions.loginAsync }
)(RegisterComponentModal);
