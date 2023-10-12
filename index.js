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

app.get('/addresses', db.routeAddresses); // list of addresses that have been geocoded, for use in Data Explorer
app.get('/hits', db.routeHits); // list database queries, for use in Data Explorer
app.get('/indexes', db.routeIndexes); // list database indexes, for use in Data Explorer
app.get('/tables', db.routeTables); // list database tables, for use in Data Explorer
app.get('/counttablesrows', db.routeCountTablesRows); // number of tables and rows, for use in Data Explorer
app.get('/countindexes', db.routeCountIndexes); // number of indexes, for use in Data Explorer
app.get('/databasesize', db.routeDatabasesize); // size of the database
app.get('/averages', db.initializeVariables, db.routeAverages); // 5-year hourly averages
app.get('/hourly', db.initializeVariables, db.routeHourly); // real hourly data
app.get('/daily', db.initializeVariables, db.routeDaily); // daily statistics

// Georgia Weather Station data for output in Data Explorer (http://aesl.ces.uga.edu/weatherapp/de)
app.get('/GAWeatherStations', db.routeGAWeatherStations);

// query for discrepancies between MRMS and NLDAS-2 precipitation.  Example: https://weather.covercrop-data.org/nvm?location=texas.
// Likely superceded by nvm2.
app.get('/nvm', db.initializeVariables, db.routeNvm);

app.get('/nvm2', db.initializeVariables, db.routeNvm2); // NLDAS-2 vs. MRMS (http://aesl.ces.uga.edu/weatherapp/src/nvm2)
app.get('/nvm2Data', db.initializeVariables, db.routeNvm2Data); // "
app.get('/nvm2Update', db.routeNvm2Update); // "
app.get('/nvm2Query', db.routeNvm2Query); // "

// query for inconsistencies between adjacent MRMS locations during 2019.
// Example: https://weather.covercrop-data.org/mvm?lat=39&lon=-76&num=100
app.get('/mvm', db.routeMvm);

app.post('/rosetta', db.routeRosetta); // bypass CORS issue of https://www.handbook60.org/api/v1/rosetta/1

app.all('/watershed', db.initializeVariables, db.routeWatershed);
app.all('/mlra', db.initializeVariables, db.routeMLRA);
app.all('/county', db.initializeVariables, db.routeCounty);
app.all('/frost', db.initializeVariables, db.routeFrost);
app.all('/countyspecies', db.routeCountySpecies);
app.all('/mlraspecies', db.routeMlraSpecies);
app.all('/mlraspecies2', db.routeMlraSpecies2);
app.all('/mlraerrors', db.routeMLRAErrors);
app.all('/plants', db.routePlants);
app.all('/plants2', db.routePlants2);
app.all('/plantsrecords', db.initializeVariables, db.routePlantsRecords);
app.all('/plantsstructure', db.initializeVariables, db.routePlantsStructure);
app.all('/plantstable', db.initializeVariables, db.routePlantsTable);
app.all('/plantsemptycolumns', db.initializeVariables, db.routePlantsEmptyColumns);

app.all('/yearly', db.initializeVariables, db.routeYearly);

app.get('/test', db.initializeVariables, db.routeTest);

app.listen(80);

console.log('Running!');
console.log('_'.repeat(process.stdout.columns));
