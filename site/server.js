var express = require('express'),
  app = express(),
  port = process.env.PORT || 3000,
  mongoose = require('mongoose'),
  User = require('./api/models/userModel'),
   Component = require('./api/models/componentModel'),
    LatestRelease = require('./api/models/latestReleaseModel'),
  bodyParser = require('body-parser');
  
// mongoose instance connection url connection
mongoose.Promise = global.Promise;
mongoose.connect('mongodb://localhost/parapet'); 


app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());


var userRoutes = require('./api/routes/userRoutes');
var componentRoutes = require('./api/routes/componentRoutes');
app.use("/api/v1", userRoutes);
app.use("/api/v1", componentRoutes);


app.listen(port);


console.log('Parapet server has started on: ' + port);