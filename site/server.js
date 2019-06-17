var express = require('express'),
  app = express(),
  port = process.env.PORT || 3000,
  mongoose = require('mongoose'),
  User = require('./api/models/userModel'),
   Component = require('./api/models/componentModel'),
    ComponentMeta = require('./api/models/componentMetaModel'),
    LatestRelease = require('./api/models/latestReleaseModel'),
    Review = require('./api/models/reviewModel'),
  bodyParser = require('body-parser');
  
// mongoose instance connection url connection
mongoose.Promise = global.Promise;
mongoose.connect('mongodb://localhost/parapet', { useNewUrlParser: true}); 

app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());


var userRoutes = require('./api/routes/userRoutes');
var componentRoutes = require('./api/routes/componentRoutes');
var reviewRoutes = require('./api/routes/reviewRoutes');
app.use("/api/v1", userRoutes);
app.use("/api/v1", componentRoutes);
app.use("/api/v1", reviewRoutes);

app.listen(port);


console.log('Parapet server has started on: ' + port);