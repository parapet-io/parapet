'use strict';
const uuidv4 = require('uuid/v4');
var mongoose = require('mongoose');
var Schema = mongoose.Schema;
var User = require('./userModel');

// var IdType = {
//         type: String,
//         required: true,
//         unique: true,
//         default: uuidv4() 
//     };


var EventType = {
    name: String,
    description: String
};


var InterfaceType = {
    input: { type: [EventType] },
    output: { type: [EventType] }
};

// Validators

function hasField(name) {
    return function(v) {
        return typeof(v[name]) !== "undefined";
    }
}

function hasValue(name) {
    return function(v) {
        var value = v[name];
        if (value === null) return false;
        // because Object.entries(new Date()).length === 0;
        // we have to do some additional check
        if (typeof value === 'object') return value.constructor === Object && Object.entries(value).length !== 0;
        return true;
    }
}

function hasEvents(name) {
    return function(v) {
        return hasField(name)(v['interface'])
    }
}

function validateEvents(name) {
    return function(v) {
        var events = v['interface'][name];
        console.log(events);
        if (events.length == 0) {
            return true;
        } else {
            for (var i = 0; i < events.length; i++) {

                if (
                    !hasField('name')(events[i]) ||
                    !hasValue('name')(events[i]) ||
                    !hasField('description')(events[i]) ||
                    !hasValue('description')(events[i])
                ) return false;
            }
        }
        return true;
    }
}

function requiredFieldError(name) {
    return `'${name}' field is required`
}

function emptyValueError(name) {
    return `'${name}' field cannot be empty`
}

var specificationValidators = [
    { validator: hasField("interface"), msg: requiredFieldError("interface") },
    { validator: hasValue("interface"), msg: emptyValueError("interface") },
    { validator: hasEvents("input"), msg: "interface object must have 'input' field" },
    { validator: hasEvents("output"), msg: "interface object must have 'output' field" },
    { validator: validateEvents("input"), msg: "invalid  'input' object. 'name' and 'description' are required fields" },
    { validator: validateEvents("output"), msg: "invalid  'output' object. 'name' and 'description' are required fields" }
];

var componentSchema = new Schema({
     
   // id: IdType,

    token: {
        type: String,
        required: true,
        select: false,
        validate: {
            isAsync: true,
            validator: function(v, cb) {
                User.count({
                    token: v
                }, function(err, count) {
                    cb(count == 1, `User with '${v}' token doesn't exist!`);
                });
            }
        }
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

    releaseDate: {
        type: Date,
        default: Date.now
    },

    sourceCodeRepoUrl: {
        type: String
    },

    homepage: {
        type: String
    },

    specification: {
        type: {
            interface: InterfaceType,
        },
        required: true,
        validate: specificationValidators
    },

    artifact: {
        repositoryUrl: {
            type: String,
            required: true
        },
        groupId: {
            type: String,
            required: true
        },
        artifactId: {
            type: String,
            required: true
        },
        version: {
            type: String,
            required: true
        }
    },
    tags: [String]

}, { versionKey: false });

module.exports = mongoose.model('Component', componentSchema);