'use strict';
var mongoose = require('mongoose');
var Schema = mongoose.Schema;

var userSchema = new Schema({
    email: {
        type: String,
        required: true,
         unique : true
    },
    password: {
        type: String,
        required: true,
        select: false
    },
    token: {
        type: String,
        required: true,
        unique : true
    },
    created_date: {
        type: Date,
        default: Date.now
    },

}, { versionKey: false });

module.exports = mongoose.model('User', userSchema);