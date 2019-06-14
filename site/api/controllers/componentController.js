'use strict';
var mongoose = require('mongoose');
var Component = mongoose.model('Component');
var ComponentMeta = mongoose.model('ComponentMeta');
var LatestRelease = mongoose.model('LatestRelease');
const uuidv4 = require('uuid/v4');

exports.register = function(req, res) {
    var payload = req.body;
    payload.token = uuidv4();
    var newComponent = new Component(payload);
    newComponent.save(function(err, component) {
        if (err) {
            res.send(err);
        } else {
            res.json(component);
        }
    });
};

exports.publish = function(req, res) {
    Component.findOne({ token: req.body.token }, 'name', function(err, component) {
        console.log("component: " + component);
        if (component == null) {
            res.status(404).json({ error: `Component with '${req.body.token}' token doesn't exist` });
        } else {
            var newComponentMeta = new ComponentMeta(req.body);
            newComponentMeta.id = new mongoose.Types.ObjectId();
            var latestRelease = LatestRelease({
                'token': newComponentMeta.token,
                'componentMetaId': newComponentMeta.id,
                'name': component.name,
                'version': newComponentMeta.artifact.version,
            });

            var error = latestRelease.validateSync();
            if (error) {
                res.send(error);
            } else {
                newComponentMeta.save(function(err, component) {
                    if (err) {
                        res.send(err);
                    } else {
                        var replacement = latestRelease.toObject();
                        delete replacement._id;
                        LatestRelease.replaceOne({ token: component.token }, replacement, { upsert: true },
                            function(n, nModified, error) {
                                if (error)
                                    res.send(new Error("failed to update latestReleasesCollection for token:" + newComponent.token));
                                res.json(component);
                            });
                    }

                });
            }
        }
    });

};

exports.searchByName = function(req, res) {
    LatestRelease.find({ name: new RegExp(req.params.name, "i") })
        .select({ componentId: 1, name: 1, version: 1 })
        .limit(10)
        .exec(function(err, docs) {
            if (err)
                res.send(err);
            res.json(docs);
        });

};

exports.delete = function(req, res) {
    var id = req.params.id;
    Component.findByIdAndRemove(id)
        .then(component => {
            if (component == null) {
                res.status(404).json({ message: `component with id = '${id}' not found` }) // todo don't send response here
                return Promise.reject(new Error(`component with id = '${id}' not found`)) //  use custom errors
            } else {
                return ComponentMeta.deleteMany({ token: component.token }).then(() => component.token)
            }
        })
        .then(token => {
            console.log("delete LatestRelease: "  +  token);
            return LatestRelease.deleteMany({ token: token });
        })
        .then(() => res.json({ message: `component with id = '${id}' deleted` }))
        .catch(console.error); // todo send response here. use custom errors (code,  message) or whatever 
};