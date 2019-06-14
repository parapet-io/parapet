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
    componentMetaId: {
        type: ObjectId,
        required: true,
    },
    version: {
        type: String,
        required: true
    }

}, { versionKey: false });

latestReleaseSchema.index({ name: "text" });

module.exports = mongoose.model('LatestRelease', latestReleaseSchema);