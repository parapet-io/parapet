'use strict';

var mongoose = require('mongoose');
var Schema = mongoose.Schema;

var componentSchema = new Schema({

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