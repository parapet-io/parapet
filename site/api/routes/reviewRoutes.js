'use strict';
var express = require('express');
var review = require('../controllers/reviewController');
var router = express.Router();


// User Routes
router.get('/reviews/:id', review.getReviews);
router.post('/reviews', review.submit);
router.delete('/reviews/:id', review.delete);

module.exports = router;