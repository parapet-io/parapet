'use strict';
var express = require('express');
var component = require('../controllers/componentController');
var router = express.Router();

router.get("/components/:id", component.getComponent)
router.get("/components/getByUserId/:id", component.getComponentByUserId)
router.get("/components/searchByName/:name", component.searchByName)
router.post('/components', component.register);
router.post('/components/publish', component.publish);
router.delete('/components/:id', component.delete);
router.get("/components/meta/:id", component.getMeta);

module.exports = router;