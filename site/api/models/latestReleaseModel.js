'use strict';
var mongoose = require('mongoose');
var Schema = mongoose.Schema;

var ObjectId = mongoose.Schema.Types.ObjectId;

var latestReleaseSchema = new Schema({
    token: {
        type: String,
        required: true,
        unique: true
    },
    name: {
        type: String,
        required: true
    },
    componentId: {
        type: ObjectId,
        required: true,
    },

    version: {
        type: String,
        required: true
    },
    tags: [String]


}, { versionKey: false });

latestReleaseSchema.index({ name: "text" });

module.exports = mongoose.model('LatestRelease', latestReleaseSchema);