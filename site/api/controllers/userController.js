'use strict';

const uuidv4 = require('uuid/v4');

var mongoose = require('mongoose'),
    User = mongoose.model('User');

exports.getUsers = function(req, res) {
    User.find({}, function(err, user) {
        if (err)
            res.send(err);
        res.json(user);
    });
};

exports.createUser = function(req, res) {
    var newUser = new User(req.body);
    newUser.save(function(err, user) {
        if (err)
            res.send(err);
        res.json(user);
    });
};


exports.getUser = function(req, res) {
    User.findById(req.params.userId, function(err, user) {
        if (err)
            res.send(err);
        res.json(user);
    });
};

exports.updateUser = function(req, res) {
    User.findOneAndUpdate({ _id: req.params.userId }, req.body, { new: true }, function(err, user) {
        if (err)
            res.send(err);
        res.json(user);
    });
};

exports.deleteUser = function(req, res) {


    User.remove({
        _id: req.params.userId
    }, function(err, user) {
        if (err)
            res.send(err);
        res.json({ message: 'User has been successfully deleted' });
    });
};