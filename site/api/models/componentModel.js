'use strict';

var mongoose = require('mongoose');
var Schema = mongoose.Schema;
var User = require('./userModel');

var componentSchema = new Schema({

    userId: {
        type: mongoose.Schema.Types.ObjectId,
        required: true,
        index: true,
        validate: {
            isAsync: true,
            validator: function(v, cb) {
                User.count({
                    _id: v
                }, function(err, count) {
                    cb(count == 1, `User with '${v}' id doesn't exist`);
                });
            }
        }
    },

    token: {
        type: String,
        required: true,
        unique: true
    },

    name: {
        type: String,
        required: true,
        maxlength: 50
    },

    description: {
        type: String,
        maxlength: 260
    },

    author: {
        type: String,
        maxlength: 50,
        required: true
    },

    sourceCodeRepoUrl: {
        type: String
    },

    homepage: {
        type: String
    },

    license: {
        type: String,
        default: "None"
    },

    tags: [String]

}, { versionKey: false });

module.exports = mongoose.model('Component', componentSchema);