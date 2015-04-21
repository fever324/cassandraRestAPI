// server.js

// BASE SETUP
// =============================================================================

// call the packages we need
var express    = require('express');        // call express
var app        = express();                 // define our app using express
var bodyParser = require('body-parser');
var cassandra  = require('cassandra-driver');
var async      = require('async')

// get in contact with cassandra
var ip = '52.5.253.89';


var client = new cassandra.Client( { contactPoints : [ip], keyspace: 'project' } );
//var client = new cassandra.Client( { contactPoints : [ip] } );
client.connect(function(err, result) {
    if(err){
        console.log(err)
    } else {
        console.log('Connected.');    
    }
    
});

// Queries
var getAllPrediction = 'SELECT * FROM project.prediction;';
var getPredictionByticker = 'SELECT * FROM prediction WHERE ticker = ? ALLOW FILTERING;';
var insertStockPrediction = 'INSERT INTO prediction (ticker, date ,close,difference,humidity,maxtemperature,mintemperature,precipitation,prediction,predictionprobability,winddirdegrees) VALUES (?,?,?,?,?,?,?,?,?,?,?);'
var getPredictionBytickerAndTimeRange = 'SELECT * FROM prediction WHERE ticker = ? AND date > ? AND date < ? allow filtering;';
var getPredictionBytickerAndStartDate = 'SELECT * FROM prediction WHERE ticker = ? AND date > ? allow filtering;';

// configure app to use bodyParser()
// this will let us get the data from a POST
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

var port = process.env.PORT || 8080;        // set our port

// ROUTES FOR OUR API
// =============================================================================
var router = express.Router();              // get an instance of the express Router

// Canssandra stuff
// test route to make sure everything is working (accessed at GET http://localhost:8080/api)
router.get('/metadata', function(req, res) {
    //res.json({ message: 'hooray! welcome to our api!' });   
    res.send(client.hosts.slice(0).map(function (node) {
        return { address : node.address, rack : node.rack, datacenter : node.datacenter }
    }));
});

app.post('/keyspace', function(req, res) {
    client.execute("CREATE KEYSPACE IF NOT EXISTS project WITH replication " + 
                   "= {'class' : 'SimpleStrategy', 'replication_factor' : 3};",
                   afterExecution('Error: ', 'Keyspace created.', res));
});



function afterExecution(errorMsessage, successMessage, res) {
    return function(err) {
        if (err) {
            console.log("error!!!!")
            console.log(err)
            return errorMsessage
            //return res.json(errorMsessage);
        } else {
            res.json(successMessage);
        }
    }
}

app.post('/tables', function(req, res) {
    async.parallel([

//"Ticker", "Date", "Close", "Difference", "Max TemperatureF", "Mean TemperatureF", "Min TemperatureF", "MeanDew PointF", "Mean Humidity",  "Mean VisibilityMiles", "PrecipitationIn", "CloudCover", "WindDirDegrees
        function(next) {
            client.execute('CREATE TABLE IF NOT EXISTS project.prediction (' +
                'Ticker text,' +
                'Date timestamp,' + 
                'Close float,' +
                'Difference float,' +
                'MaxTemperature int,' +
                'MinTemperature int,' +
                'WindDirDegrees int,' + 
                'Precipitation float,' + 
                'Humidity int,' +
                'Prediction boolean,' +
                'PredictionProbability float,' +
                'PRIMARY KEY((Ticker), Date)' +
                ');',
                next);
            
        }
    ], afterExecution('Error: ', 'Tables created.' , res));
});




app.post('/prediction', function(req, res) {
    //(ticker, date ,close,difference,humidity,maxtemperature,mintemperature,precipitation,prediction,predictionprobability,winddirdegrees)
    client.execute(insertStockPrediction,
        [req.body.ticker, req.body.date, req.body.close, req.body.difference, req.body.humidity,
         req.body.maxtemperature, req.body.mintemperature, req.body.precipitation, req.body.prediction,
         req.body.predictionprobability, req.body.winddirdegrees],
        afterExecution('Error: ', req.body.ticker + ' prediction at' + req.body.time + ' inserted.', res));
});

app.get('/prediction/:ticker', function(req, res) {
    client.execute(getPredictionByticker, [ req.params.ticker ], function(err, result) {
        if (err) {
            console.log(err)
            res.status(404).send({ msg : 'Stock not found.' });
        } else {
            res.json(result.rows);    
        }
    });
});

app.get('/prediction/:ticker/:startdate', function(req, res) {
	var startdate = new Date(req.params.startdate);
    client.execute(getPredictionBytickerAndStartDate, [ req.params.ticker, startdate], function(err, result) {
        if (err) {
            console.log(err)
            res.status(404).send({ msg : 'Stock not found.' });
        } else {
            res.json(result.rows);    
        }
    });
});

app.get('/prediction/:ticker/:startdate/:enddate', function(req, res) {
	var startdate = new Date(req.params.startdate);
	var enddate = new Date(req.params.enddate);
    client.execute(getPredictionBytickerAndTimeRange, [ req.params.ticker,startdate,enddate ], function(err, result) {
        if (err) {
            console.log(err)
            res.status(404).send({ msg : 'Stock not found.' });
        } else {
            res.json(result.rows);    
        }
    });
});



app.get('/prediction', function(req, res) {
    client.execute(getAllPrediction, function(err, result) {
        if (err) {
            console.log(err)
            res.status(404).send({ msg : 'No prediction found.' });
        } else {
            res.json(result.rows);    
        }
    });
});



// START THE SERVER
// =============================================================================
app.listen(port);
console.log('Magic happens on port ' + port);

