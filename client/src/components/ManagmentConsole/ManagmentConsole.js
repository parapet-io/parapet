import React, { useState } from "react";

import { makeStyles } from "@material-ui/core/styles";
import Table from "@material-ui/core/Table";
import TableBody from "@material-ui/core/TableBody";
import TableCell from "@material-ui/core/TableCell";
import TableHead from "@material-ui/core/TableHead";
import TableRow from "@material-ui/core/TableRow";
import Paper from "@material-ui/core/Paper";
import Fab from "@material-ui/core/Fab";
import AddIcon from "@material-ui/icons/Add";

import RegisterComponentModal from "./RegisterComponentModal";

import s from "./ManagmentConsole.module.scss";

const ManagmentConsole = () => {
  const [isRegisterOpen, setIsRegisterOpen] = useState(false);

  const handleRegisterClose = () => setIsRegisterOpen(false);
  const handleRegisterOpen = () => setIsRegisterOpen(true);

  function createData(name, tags, version, token) {
    return { name, tags, version, token };
  }

  const rows = [createData("Test", "tag", 1.2, "sdgklnifdsghisdbihfbqiexf")];

  const useStyles = makeStyles(theme => ({
    paper: {
      marginTop: theme.spacing(3),
      width: "100%",
      overflowX: "auto"
    },
    table: {
      minWidth: 650
    },
    margin: {
      margin: theme.spacing(1),
      width: "300px"
    },
    extendedIcon: {
      marginRight: theme.spacing(1)
    }
  }));

  const classes = useStyles();
  return (
    <div className={s.managmentWrapper}>
      <Fab
        variant="extended"
        color="primary"
        aria-label="Add"
        className={classes.margin}
        onClick={handleRegisterOpen}
      >
        <AddIcon className={classes.extendedIcon} />
        Register component
      </Fab>

      <Paper className={classes.paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell align="right">Tags</TableCell>
              <TableCell align="right">Latest release version</TableCell>
              <TableCell align="right">Token</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {rows.map(row => (
              <TableRow key={row.name}>
                <TableCell component="th" scope="row">
                  {row.name}
                </TableCell>
                <TableCell align="right">{row.tags}</TableCell>
                <TableCell align="right">{row.version}</TableCell>
                <TableCell align="right">{row.token}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </Paper>
      <RegisterComponentModal
        open={isRegisterOpen}
        handleClose={handleRegisterClose}
      />
    </div>
  );
};

export default ManagmentConsole;
