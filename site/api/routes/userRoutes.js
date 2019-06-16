'use strict';
var express = require('express');
var user = require('../controllers/userController');
var router = express.Router();


// User Routes
router.get('/users', user.getUsers);
router.post('/users', user.createUser);
router.post("/login", user.login);

router.get('/users/:userId', user.getUser);
router.put('/users/:userId', user.updateUser);
router.delete('/users/:userId', user.deleteUser);

module.exports = router;