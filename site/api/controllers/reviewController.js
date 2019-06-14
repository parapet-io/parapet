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
    Review.find({ componentId: req.params.id }, function(err, reviews) {
        if (err)
            res.send(err);
        res.json(reviews);
    });
};

exports.delete = function(req, res) {
    Review.findOneAndDelete(req.id)
        .then(review => {
            if (review == null) {
                res.status(404).json({ message: `review with id='${req.id}' not found` });
            } else {
                res.json({ message: "review has been deleted" })
            }
        })
        .catch(err => res.status(500).json({ message: "failed to delete review:  " + err }))

};