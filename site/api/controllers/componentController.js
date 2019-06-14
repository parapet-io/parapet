'use strict';
var mongoose = require('mongoose'),
    Component = mongoose.model('Component'),
    LatestRelease = mongoose.model('LatestRelease');

exports.publish = function(req, res) {
    var newComponent = new Component(req.body);
    newComponent.id = new mongoose.Types.ObjectId();
    var latestRelease = LatestRelease({
        'token': newComponent.token,
        'componentId': newComponent.id,
        'name': newComponent.name,
        'version': newComponent.artifact.version,
        'tags': newComponent.tags
    });

    var error = latestRelease.validateSync();
    if (error) {
        res.send(error);
    } else {
        newComponent.save(function(err, component) {
            if (err) {
                res.send(err);
            } else {
                var replacement = latestRelease.toObject();
                delete replacement._id;
                LatestRelease.replaceOne({ token: component.token }, replacement, { upsert: true },
                    function(n, nModified, error) {
                        if (error)
                            res.send(new Error("failed to updated latestReleasesCollection for token:" + newComponent.token));
                        res.json(component);
                    });
            }

        });
    }
};

exports.searchByName = function(req, res) {
    LatestRelease.find({ $text: { $search: req.params.name } })
        .select({ componentId: 1, name: 1, version: 1 })
        .limit(10)

        .exec(function(err, docs) {
            if (err)
                res.send(err);
            res.json(docs);
        });

};