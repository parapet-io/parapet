'use strict';
var express = require('express');
var component = require('../controllers/componentController');
var router = express.Router();

router.get("/components/searchByName/:name", component.searchByName)
router.post('/components', component.register);
router.post('/components/publish', component.publish);
router.delete('/components/:id', component.delete);


module.exports = router;