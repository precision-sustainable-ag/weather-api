const {Pool, Client} = require('pg');

const fs = require('fs');

const data = fs.readFileSync('./.env', 'utf8');

const logins = {};

data.split(/[\n\r]+/).forEach(login => {
  const [db, user, pass] = login.split('|');
  logins[db] = {user, pass};
});

const pool = new Pool({
  user      : logins.Weather.user,
  password  : logins.Weather.pass,
  zhost      : 'localhost',
  zhost      : 'weather.aesl.ces.uga.edu',
  host      : '128.192.142.200',
  database  : 'postgres',
  port      : 5432,
});

const crownPool = new Pool({
  user      : logins.Crown.user,
  password  : logins.Crown.pass,
  host      : 'onfarm-dbs.postgres.database.azure.com',
  database  : 'crowndb',
  port      : 5432,
  ssl       : true
});

const mysql = require('mysql');

const dashboardPool = mysql.createPool({
  user      : logins.Dashboard.user,
  password  : logins.Dashboard.pass,
  host      : 'new-tech-dashboard.mysql.database.azure.com',
  database  : 'tech-dashboard',
  port      : 3306,
});

const iterisKey = logins.Iteris.pass;

const mainKey = logins.MainKey.pass;

const githubKey = logins.Github.pass;

const googleAPIKey = logins.GoogleAPI.pass;

module.exports = {
  pool,
  crownPool,
  dashboardPool,
  iterisKey,
  mainKey,
  githubKey,
  googleAPIKey,
}
