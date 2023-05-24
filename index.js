// https://stackoverflow.com/questions/44629631/while-using-pandas-got-error-urlopen-error-ssl-certificate-verify-failed-cert
// const ssl = require('ssl');
// ssl._create_default_https_context = ssl._create_unverified_context

const ip = require('ip');

console.log('IP:', ip.address()); // if needed for /etc/postgresql/11/main/pg_hba.conf

process.on('uncaughtException', (err) => {
  console.error(err);
  console.log('Node NOT Exiting...');
});

const express = require('express'); // simplifies http server development
const bodyParser = require('body-parser'); // make form data available in req.body
const cors = require('cors'); // allow cross-origin requests
const path = require('path'); // to get the current path

const app = express();

app.use(cors());
app.use(bodyParser.json({ limit: '50mb' }));
app.use(bodyParser.urlencoded({ limit: '50mb', extended: true }));

// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => { // next is unused but required!
  console.error(err.stack);
  res.status(500).send('Something broke!');
});

app.use(express.static(path.join(__dirname, 'public'))); // make the public folder available

app.use(express.static(`${__dirname}/static`, { dotfiles: 'allow' })); // from Ayaan

app.use(express.static(`${__dirname}/public/client/build`));

const db = require('./db');

app.get('/', (req, res) => res.sendFile(`${__dirname}/public/client/build/index.html`)); // send API

app.get('/addresses', db.addresses); // list of addresses that have been geocoded, for use in Data Explorer
app.get('/hits', db.hits); // list database queries, for use in Data Explorer
app.get('/indexes', db.indexes); // list database indexes, for use in Data Explorer
app.get('/tables', db.tables); // list database tables, for use in Data Explorer
app.get('/counttablesrows', db.counttablesrows); // number of tables and rows, for use in Data Explorer
app.get('/countindexes', db.countindexes); // number of indexes, for use in Data Explorer
app.get('/databasesize', db.databasesize); // size of the database
app.get('/averages', db.initializeVariables, db.getAverages); // 5-year hourly averages
app.get('/hourly', db.initializeVariables, db.getHourly); // real hourly data
app.get('/daily', db.initializeVariables, db.getDaily); // daily statistics

// Georgia Weather Station data for output in Data Explorer (http://aesl.ces.uga.edu/weatherapp/de)
app.get('/GAWeatherStations', db.GAWeatherStations);

// query for discrepancies between MRMS and NLDAS-2 precipitation.  Example: http://weather.aesl.ces.uga.edu/weather/nvm?location=texas.
// Likely superceded by nvm2.
app.get('/nvm', db.initializeVariables, db.nvm);

app.get('/nvm2', db.initializeVariables, db.nvm2); // NLDAS-2 vs. MRMS (http://aesl.ces.uga.edu/weatherapp/src/nvm2)
app.get('/nvm2Data', db.initializeVariables, db.nvm2Data); // "
app.get('/nvm2Update', db.nvm2Update); // "
app.get('/nvm2Query', db.nvm2Query); // "

// query for inconsistencies between adjacent MRMS locations during 2019.
// Example: https://weather.covercrop-data.org/mvm?lat=39&lon=-76&num=100
app.get('/mvm', db.mvm);

app.post('/rosetta', db.rosetta); // bypass CORS issue of https://www.handbook60.org/api/v1/rosetta/1

app.all('/watershed', db.initializeVariables, db.watershed);
app.all('/mlra', db.initializeVariables, db.mlraAPI);
app.all('/county', db.initializeVariables, db.countyAPI);
app.all('/frost', db.initializeVariables, db.frost);
app.all('/countyspecies', db.countyspecies);
app.all('/mlraspecies', db.mlraspecies);
app.all('/mlraspecies2', db.mlraspecies2);
app.all('/mlraerrors', db.mlraerrors);
app.all('/plants', db.plants);
app.all('/plants2', db.plants2);

app.get('/test', db.test);

app.listen(80);

console.log('Running!');
console.log('_'.repeat(process.stdout.columns));
