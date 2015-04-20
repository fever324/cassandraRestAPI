// server.js

// BASE SETUP
// =============================================================================

// call the packages we need
var express    = require('express');        // call express
var app        = express();                 // define our app using express
var bodyParser = require('body-parser');
var cassandra  = require('cassandra-driver');
var async 	   = require('async')

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
var getSongById = 'SELECT * FROM simplex.songs WHERE id = ?;';
var getStockBySymbol = 'SELECT * FROM project.stock WHERE symbol = ? ALLOW FILTERING;';
var getPredictionBySymbol = 'SELECT * FROM prediction WHERE symbol = ? ALLOW FILTERING;';
var getAllStock = 'SELECT * FROM project.stock';
var getAllWeather = 'SELECT * FROM project.weather';
var getAllStock = 'SELECT * FROM project.stock';
var getAllPrediction = 'SELECT * FROM project.prediction;';
var insertWeather = 'INSERT INTO project.weather (id, time, zipcode, temperature, precipitation, dewpoint)  '
    + 'VALUES(?, ?, ?, ?, ?, ?);';

var insertStockPrediction = 'INSERT INTO prediction (id, symbol, time, prediction) VALUES (?, ?, ?, ?)'


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
        function(next) {
            client.execute('CREATE TABLE IF NOT EXISTS project.weather (' +
                'id uuid,' +
                'time timestamp,' +   
                'zipcode int,' +
                'temperature int,' +
                'precipitation int,' +
                'dewpoint int,' +
                'PRIMARY KEY (id, time, zipcode)' +
                ');',
                next);
        },
        function(next) {
            client.execute('CREATE TABLE IF NOT EXISTS project.stock (' +
                'id uuid,' +
                'symbol text,' +
                'time timestamp,' +
                'price double,' +
                'volume int,' +
                'PRIMARY KEY (id, symbol, time)' +
                ');',
                next);
        },
        function(next) {
            client.execute('CREATE TABLE IF NOT EXISTS project.prediction (' +
                'id uuid,' +
                'symbol text,' +
                'time timestamp,' +
                'prediction boolean,' +
                'PRIMARY KEY (id, symbol, time)' +
                ');',
                next);
        }

    ], afterExecution('Error: ', 'Tables created.' , res));
});




app.post('/prediction', function(req, res) {
    //var insertStock = 'INSERT INTO prediction (id, symbol, time, prediction) VALUES (?, ?, ?, ?)'
    var insertStockPrediction = 'INSERT INTO prediction (id, symbol, time, prediction) VALUES (?, ?, ?, ?)'

    var id = null;
    if ( ! req.body.hasOwnProperty('id')) {
        id = cassandra.types.uuid();
    } else {
        id = req.body.id;
    }
    var timestamp = Date.now() / 1000
    client.execute(insertStockPrediction,
        //[id, req.body.symbol, req.body.time, req.body.prediction],
        [id, req.body.symbol, timestamp, true],
        afterExecution('Error: ', req.body.symbol + ' prediction at' + req.body.time + ' inserted.', res));
});

app.get('/prediction/:symbol', function(req, res) {
    client.execute(getPredictionBySymbol, [ req.params.symbol ], function(err, result) {
        if (err) {
            console.log(err)
            res.status(404).send({ msg : 'Stock not found.' });
        } else {
            res.json(result);    
        }
    });
});

app.get('/prediction', function(req, res) {
    client.execute(getAllPrediction, function(err, result) {
        if (err) {
            console.log(err)
            res.status(404).send({ msg : 'No prediction found.' });
        } else {
            res.json(result);    
        }
    });
});


app.get('/weather', function(req, res) {
    client.execute(getAllWeather, function(err, result) {
        if (err) {
            res.status(404).send({ msg : 'Weather not found.' });
        } else {
            res.json(result);   
        }
    });
});

app.post('/weather', function(req, res) {
    var id = null;
    if ( ! req.body.hasOwnProperty('id')) {
        id = cassandra.types.uuid();
    } else {
        id = req.body.id;
    }
    client.execute(insertWeather,
        [id, req.body.time, req.body.zipcode, req.body.temperature, req.body.precipitation, req.body.dewpoint],
        afterExecution('Error: ', 'Weather at' + req.body.time + ' inserted.', res));
});

app.get('/stock', function(req, res) {
    client.execute(getAllStock, function(err, result) {
        if (err) {
            console.log(err)
            res.status(404).send({ msg : 'Stock not found.' });
        } else {
            res.json(result);   
        }
    });
});

app.get('/stock/:symbol', function(req, res) {
    client.execute(getStockBySymbol, [ req.params.symbol ], function(err, result) {
        if (err) {
            console.log(err)
            res.status(404).send({ msg : 'Stock not found.' });
        } else {
            res.json(result);    
        }
    });
});




// more routes for our API will happen here

// REGISTER OUR ROUTES -------------------------------
// all of our routes will be prefixed with /api
app.use('/api', router);

// START THE SERVER
// =============================================================================
app.listen(port);
console.log('Magic happens on port ' + port);