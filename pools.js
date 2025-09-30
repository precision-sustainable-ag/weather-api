const fs = require('fs');

const { Pool } = require('pg');

const data = fs.readFileSync('./.env', 'utf8');

const logins = {};

data.split(/[\n\r]+/).forEach((login) => {
  const [db, user, pass] = login.split('|');
  logins[db] = { user, pass };
});

const pool = new Pool({
  user: logins.Weather.user,
  password: logins.Weather.pass,
  host: '128.192.142.200',
  database: 'postgres',
  port: 5432,
});

const googleAPIKey = logins.GoogleAPI.pass;

module.exports = {
  pool,
  googleAPIKey,
};
