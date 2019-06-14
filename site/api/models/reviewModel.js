'use strict';
var mongoose = require('mongoose');
var Schema = mongoose.Schema;

var reviewSchema = new Schema({
    userId: {
        type: mongoose.Schema.Types.ObjectId,
        required: true
    },
    componentId: {
        type: mongoose.Schema.Types.ObjectId,
        required: true
    },
    text: {
        type: String,
        required: true,
        index: true
    },
    rating: {
        type: Number,
        required: true,
        min: 1,
        max: 5
    },
    createDate: {
        type: Date,
        default: Date.now
    },

}, { versionKey: false });

module.exports = mongoose.model('Review', reviewSchema);