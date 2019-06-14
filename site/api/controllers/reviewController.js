'use strict';

var mongoose = require('mongoose'),
    Review = mongoose.model('Review');

exports.submit = function(req, res) {
    var newReview = new Review(req.body);
    newReview.save(function(err, user) {
        if (err)
            res.send(err);
        res.json(user);
    });
}

exports.getReviews = function(req, res) {
    Review.find({componentId: req.params.id}, function(err, reviews) {
        if (err)
            res.send(err);
        res.json(reviews);
    });
};