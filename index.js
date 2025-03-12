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
app.use(bodyParser.json({ limit: '100mb' }));
app.use(bodyParser.urlencoded({ limit: '100mb', extended: true }));

// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => { // next is unused but required!
  console.error(err.stack);
  res.status(500).send('Something broke!');
});

app.use(express.static(path.join(__dirname, 'public'))); // make the public folder available

app.use(express.static(`${__dirname}/static`, { dotfiles: 'allow' })); // from Ayaan

app.use(express.static(`${__dirname}/public/client/build`));

const weather = require('./weather');
const vegspec = require('./vegspec');

app.get('/', (req, res) => res.sendFile(`${__dirname}/public/client/build/index.html`)); // send API

app.get('/addresses', weather.routeAddresses); // list of addresses that have been geocoded, for use in Data Explorer
app.get('/hits', weather.routeHits); // list database queries, for use in Data Explorer
app.get('/indexes', weather.routeIndexes); // list database indexes, for use in Data Explorer
app.get('/tables', weather.routeTables); // list database tables, for use in Data Explorer
app.get('/counttablesrows', weather.routeCountTablesRows); // number of tables and rows, for use in Data Explorer
app.get('/countindexes', weather.routeCountIndexes); // number of indexes, for use in Data Explorer
app.get('/databasesize', weather.routeDatabasesize); // size of the database
app.get('/averages', weather.routeAverages); // 5-year hourly averages
app.get('/hourly', weather.routeHourly); // real hourly data
app.get('/daily', weather.routeDaily); // daily statistics

// Georgia Weather Station data for output in Data Explorer (http://aesl.ces.uga.edu/weatherapp/de)
app.get('/GAWeatherStations', weather.routeGAWeatherStations);

// query for discrepancies between MRMS and NLDAS-2 precipitation.  Example: https://weather.covercrop-data.org/nvm?location=texas.
// Likely superceded by nvm2.
app.get('/nvm', weather.routeNvm);

app.get('/nvm2', weather.routeNvm2); // NLDAS-2 vs. MRMS (http://aesl.ces.uga.edu/weatherapp/src/nvm2)
app.get('/nvm2Data', weather.routeNvm2Data); // "
app.get('/nvm2Update', weather.routeNvm2Update); // "
app.get('/nvm2Query', weather.routeNvm2Query); // "

// query for inconsistencies between adjacent MRMS locations during 2019.
// Example: https://weather.covercrop-data.org/mvm?lat=39&lon=-76&num=100
app.get('/mvm', weather.routeMvm);

app.post('/rosetta', weather.routeRosetta); // bypass CORS issue of https://www.handbook60.org/api/v1/rosetta/1

app.all('/hardinesszone', weather.routeHardinessZone);

app.all('/watershed', weather.routeWatershed);
app.all('/county', weather.routeCounty);
app.all('/frost', weather.routeFrost);
app.all('/mlra', weather.routeMLRA);
app.all('/yearly', weather.routeYearly);
app.all('/yearlyprecipitation', weather.routeYearlyPrecipitation);

app.get('/test', weather.routeTest);
app.get('/elevation', weather.routeElevation);

// Vegspec
app.all('/plantsrecords', vegspec.routeRecords);
app.all('/vegspec/records', vegspec.routeRecords);

app.all('/plantscharacteristics', vegspec.routeCharacteristics);
app.all('/vegspec/characteristics', vegspec.routeCharacteristics);

app.all('/plantsstructure', vegspec.routeStructure);
app.all('/vegspec/structure', vegspec.routeStructure);

app.all('/plantstable', vegspec.routePlantsTable);
app.all('/vegspec/table', vegspec.routePlantsTable);

app.all('/plantsemptycolumns', vegspec.routePlantsEmptyColumns);
app.all('/vegspec/emptycolumns', vegspec.routePlantsEmptyColumns);
app.all('/vegspec/props', vegspec.routeProps);
app.all('/vegspec/symbols', vegspec.routeSymbols);
app.all('/vegspec/newspecies', vegspec.routeNewSpecies);
app.all('/vegspec/renamecultivar', vegspec.routeRenameCultivar);

app.all('/vegspec/savestate', vegspec.routeSaveState);
app.all('/vegspec/state', vegspec.routeState);
app.all('/vegspec/deletestate', vegspec.routeDeleteState);
app.all('/vegspec/editstate', vegspec.routeEditState);
app.all('/vegspec/missingcultivars', vegspec.routeMissingCultivars);
app.all('/vegspec/missingcharacteristics', vegspec.routeMissingCharacteristics);
app.all('/vegspec/movecultivar', vegspec.routeMoveCultivar);
app.all('/vegspec/databasechanges', vegspec.routeDatabaseChanges);
app.all('/vegspec/retention', vegspec.routeRetention);
app.all('/vegspec/validstates', vegspec.routeValidStates);
app.all('/vegspec/dataerrors', vegspec.routeDataErrors);

app.listen(80);

console.log('Running!');
console.log('_'.repeat(process.stdout.columns));
