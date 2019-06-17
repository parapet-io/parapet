import React, { useState } from "react";
import { connect } from "react-redux";

import { componentActions } from "bus/component/actions";
import { user } from "bus/auth/selectors";

import { makeStyles } from "@material-ui/core/styles";
import Dialog from "@material-ui/core/Dialog";
import DialogActions from "@material-ui/core/DialogActions";
import DialogContent from "@material-ui/core/DialogContent";
import Button from "@material-ui/core/Button";
import DialogTitle from "@material-ui/core/DialogTitle";
import TextField from "@material-ui/core/TextField";
import InputLabel from "@material-ui/core/InputLabel";
import MenuItem from "@material-ui/core/MenuItem";
import TagsInput from "react-tagsinput";
import FormControl from "@material-ui/core/FormControl";
import Select from "@material-ui/core/Select";

import "react-tagsinput/react-tagsinput.css";

const RegisterComponentModal = ({
  open,
  handleClose,
  registerComponentAsync,
  user
}) => {
  const [values, setValues] = useState({
    name: "",
    description: "",
    author: "",
    license: "",
    repoUrl: "",
    homepage: ""
  });
  const [tags, setTags] = useState([]);

  const handleTags = tags => setTags(tags);

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
      description: "",
      author: "",
      license: "",
      repoUrl: "",
      homepage: ""
    }) || setTags([]);

  const validateForm = () => values.name && values.author && values.license;

  const handleCloseForm = () => clearForm() || handleClose();

  const onFormSubmit = () => {
    if (validateForm()) {
      const { name, description, author, license, repoUrl, homepage } = values;

      const credentials = {
        userId: user._id,
        name,
        description,
        author,
        license,
        sourceCodeRepoUrl: repoUrl,
        homepage,
        tags
      };
      registerComponentAsync(credentials);
    }
  };

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
          <TagsInput value={tags} onChange={handleTags} />
        </div>
      </DialogContent>
      <DialogActions>
        <Button onClick={handleCloseForm} color="primary">
          Cancel
        </Button>
        <Button onClick={onFormSubmit} color="primary">
          Register component
        </Button>
      </DialogActions>
    </Dialog>
  );
};

const MSTP = state => ({
  user: user(state)
});
export default connect(
  MSTP,
  { registerComponentAsync: componentActions.registerComponentAsync }
)(RegisterComponentModal);
