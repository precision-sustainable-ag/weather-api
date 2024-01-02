/*
  SELECT distinct b.plant_symbol as current_symbol, a.plant_symbol AS state_symbol
  FROM (
    SELECT plant_symbol, state_code, b.* FROM (
      SELECT * FROM mlra_species a
      LEFT JOIN plants3.plant_master_tbl b
      USING (plant_symbol)
    ) a
    JOIN plants3.plant_synonym_tbl b
    ON a.plant_master_id = synonym_plant_master_id
  ) a
  JOIN plants3.plant_master_tbl b
  USING (plant_master_id);

  UPDATE mlra_species SET plant_symbol = 'SYEL2' WHERE plant_symbol = 'SYEL';
  UPDATE mlra_species SET plant_symbol = 'EUFI14' WHERE plant_symbol = 'EUFI2';
  UPDATE mlra_species SET plant_symbol = 'LIPUM2' WHERE plant_symbol = 'LIMU';
  UPDATE mlra_species SET plant_symbol = 'ARTRV' WHERE plant_symbol = 'SEVA';
  UPDATE mlra_species SET plant_symbol = 'GLMA4' WHERE plant_symbol = 'GLSO80';
  UPDATE mlra_species SET plant_symbol = 'SOBID' WHERE plant_symbol = 'SOBIS';
  UPDATE mlra_species SET plant_symbol = 'OEFI3' WHERE plant_symbol = 'GAFI2';
  UPDATE mlra_species SET plant_symbol = 'SCAR7' WHERE plant_symbol = 'SCPH';
  UPDATE mlra_species SET plant_symbol = 'OEGA' WHERE plant_symbol = 'GABI2';
  UPDATE mlra_species SET plant_symbol = 'NEAT' WHERE plant_symbol = 'PAAT';
  UPDATE mlra_species SET plant_symbol = 'EUPU21' WHERE plant_symbol = 'EUPU10';
  UPDATE mlra_species SET plant_symbol = 'TRRI' WHERE plant_symbol = 'TRRI8';
  UPDATE mlra_species SET plant_symbol = 'EUMA9' WHERE plant_symbol = 'EUMA12';
*/

/*
  SELECT * INTO plants3.nativity
  FROM (
    SELECT DISTINCT
      plc.plant_master_id,
      COALESCE(dpn.plant_nativity_id, 0) AS plant_nativity,
      COALESCE(plant_excluded_location_ind, false) AS plant_excluded_ind,
      COALESCE(pl.plant_nativity_region_id, 0) AS plant_nativity_region_id,
      dpnr.plant_nativity_region_name,
      COALESCE(plant_nativity_type, '') AS plant_nativity_type,
      COALESCE(plant_nativity_name, '') AS plant_nativity_name,
      country_identifier,
      ROW_NUMBER() OVER (PARTITION BY plc.plant_master_id, dpnr.plant_nativity_region_name
                        ORDER BY plant_nativity_type, dpnr.plant_nativity_region_name ASC) AS rn
    FROM
      plants3.plant_location_characteristic plc
      INNER JOIN plants3.plant_location pl ON pl.plant_location_id = plc.plant_location_id
      INNER JOIN plants3.d_plant_nativity dpn ON plc.plant_nativity_id = dpn.plant_nativity_id
      INNER JOIN plants3.d_plant_nativity_region dpnr ON pl.plant_nativity_region_id = dpnr.plant_nativity_region_id
    ORDER BY 1
  ) alias;

  CREATE TABLE plants3.states (
    state VARCHAR(255),
    plant_symbol VARCHAR(10),
    cultivar_name VARCHAR(20),
    parameter VARCHAR(30),
    value VARCHAR(255),
    notes TEXT
  );

  CREATE TABLE weather.canada30year (
    lat NUMERIC,
    lon NUMERIC,
    fff_0 NUMERIC,
    fff_2 NUMERIC,
    fff_4 NUMERIC,
    lsf_0 NUMERIC,
    lsf_2 NUMERIC,
    lsf_4 NUMERIC,
    fff_0_date DATE,
    fff_2_date DATE,
    fff_4_date DATE,
    lsf_0_date DATE,
    lsf_2_date DATE,
    lsf_4_date DATE
  );
*/

const { format } = require('sql-formatter');
const axios = require('axios');
const myip = require('ip');
const { pool, googleAPIKey } = require('./pools');

let lats;
let lons;
let cols;
let minLat;
let maxLat;
let minLon;
let maxLon;
let rect;
let location;
let options;
let output;
let where;
let stats;
let group;
let mrms; // true if MRMS precip; false if NLDAS precip
let attr;
let years;
let ip;
let testing;
let tests;
let testRequest;
let testResponse;

const parms = [
  'date',
  'lat',
  'lon',
  'air_temperature',
  'humidity',
  'relative_humidity',
  'pressure',
  'zonal_wind_speed',
  'meridional_wind_speed',
  'wind_speed',
  'longwave_radiation',
  'convective_precipitation',
  'potential_energy',
  'potential_evaporation',
  'shortwave_radiation',
  'precipitation',
];

/**
 * Restricts a number to a specified range.
 *
 * @param {number} value - The number to be clamped.
 * @param {number} min - The minimum value of the range.
 * @param {number} max - The maximum value of the range.
 * @returns {number} The clamped value.
 */
const clamp = (value, min, max) => Math.min(Math.max(+value, min), max);

/**
 * Creates an array of numbers within a specified range.
 *
 * @param {number} start - The start of the range.
 * @param {number} end - The end of the range (inclusive).
 * @returns {Array<number>} - The array of numbers within the specified range.
 */
const range = (start, end) => {
  const result = [];
  for (let i = start; i <= end; i++) {
    result.push(i);
  }

  return result;
}; // range

const send = (res, results, opt = {}) => {
  if (testing) {
    if (typeof results === 'object') {
      res.write('SUCCESS');
    } else {
      res.write(results);
    }
    // res.write(`\n${'_'.repeat(200)}\n`);

    if (!tests.length) {
      res.write('\nFinished');
      testing = false;
      res.end();
    } else {
      testing = true;
      res.write(`\n${tests[0].name.padEnd(25)}: `);
      tests.shift()();
    }
  } else if (output === 'html') {
    if (!Array.isArray(results)) {
      results = [results];
    }

    res.send(`
      <link rel="stylesheet" href="/css/dbGraph.css">
      <link rel="stylesheet" href="/css/weather.css">
      <style>
        table {
          position: relative;
          overflow: hidden;
        }

        tr {
          vertical-align: top;
          position: relative;
        }

        tr.even {
          background: #efc;
        }

        td:nth-child(1)[rowspan] {
          position: relative;
        }

        tr.even td:nth-child(1)::before {
          content: '';
          position: absolute;
          height: 100%;
          top: 0;
          left: 0;
          width: 100vw;
          outline: 1px solid #666;
          z-index: 999;
        }

        a {
          position: absolute;
          z-index: 1000;
        }
      </style>

      <div id="Graph"></div>

      <table id="Data">
        <thead>
          <tr><th>${Object.keys(results[0]).join('<th>')}</tr>
        </thead>
        <tbody>
          ${results.map((r) => `<tr><td>${Object.keys(r).map((v) => r[v]).join('<td>')}`).join('\n')}
        </tbody>
      </table>

      ${opt.rowspan ? `
        <script>
          const data = document.querySelector('#Data tbody');
          let cname = 'odd';
          [...data.rows].forEach((r1, i) => {
            for (let n = 0; n < data.rows[0].cells.length; n++) {
              if (n === 0 && r1.cells[0].style.display) continue;

              if (n === 0 && !r1.className) r1.classList.add(cname);

              for (let j = i + 1; j < data.rows.length; j++) {
                if ((n > 0) && (j - i + 1 > (r1.cells[0].rowSpan || 1))) {
                  break;
                }
                const r2 = data.rows[j];
                if (r1?.cells[n]?.innerText === r2?.cells[n]?.innerText) {
                  if (n === 0) r2.classList.add(cname);
                  r1.cells[n].rowSpan = j - i + 1;
                  r2.cells[n].style.display = 'none';
                } else {
                  break;
                }
              }
              
              if ((n === 0) && !r1.cells[0].style.display) {
                cname = cname === 'odd' ? 'even' : 'odd';
              }
            }
          });
        </script>` : ''}
    `);
  } else if (output === 'csv') {
    if (!Array.isArray(results)) {
      results = [results];
    }

    const s = `${Object.keys(results[0]).toString()}\n${
      results.map((r) => Object.keys(r).map((v) => r[v])).join('\n')}`;

    res.set('Content-Type', 'text/csv');
    res.setHeader('Content-disposition', `attachment; filename=${lats}.${lons}.csv`);
    res.send(s);
  } else {
    res.send(results);
  }
}; // send

/**
 * Logs a message along with the line number where the debug function is called.
 * @param {string} s - The message to log.
 * @returns {void}
 */
const debug = (s, res, status = 200) => {
  try {
    throw new Error();
  } catch (error) {
    // Extract the stack trace
    const stackLines = error.stack.split('\n');

    let lineNumber;
    // Find the line number
    try {
      lineNumber = parseInt(stackLines[2].match(/at.*\((.*):(\d+):\d+\)/)[2], 10);
    } catch (err) {
      lineNumber = '';
    }

    const result = `
      Line ${lineNumber}
${JSON.stringify(s, null, 2).replace(/\\n/g, '\n')}
    `.trim();

    console.log(result);
    console.log('_'.repeat(process.stdout.columns));

    if (res && !testing) {
      res.type('text/plain');
      res.status(status).send(result);
    } else if (res && testing) {
      send(res, `ERROR\n${result}\n`);
    }
  }
}; // debug

/**
 * Initializes various parameters based on the given request object.
 *
 * @param {Object} req - The request object.
 * @returns {undefined}
 */
const init = (req) => {
  ip = (req.headers['x-forwarded-for'] || '').split(',').pop() || req.socket.remoteAddress;

  output = req.query.explain ? 'json' : req.query.output ?? 'json';

  lats = null;
  lons = null;
  cols = null;
  minLat = null;
  maxLat = null;
  minLon = null;
  maxLon = null;
  location = (req.query.location || '').replace(/[^a-z0-9 ]/ig, '').replace(/\s+/g, ' ').toLowerCase();
  options = (req.query.options || '').toLowerCase().split(',');
  rect = options.includes('rect') && (location || (req.query.lat || '').split(',').length === 2);

  debug({
    lats,
    lons,
    cols,
    minLat,
    maxLat,
    minLon,
    maxLon,
    rect,
    location,
    options,
    output,
    ip,
  });
}; // init

/** ____________________________________________________________________________________________________________________________________
 * Round latitude and longitude values to the nearest NLDAS-2 grid coordinates used in the database.
 *
 * @param {number} n - The latitude or longitude value to be rounded.
 * @returns {string} The rounded NLDAS-2 grid coordinate as a string with three decimal places.
 */
const NLDASlat = (n) => -(Math.floor(-n * 8) / 8).toFixed(3);
const NLDASlon = (n) => (Math.floor(n * 8) / 8).toFixed(3);

/** ____________________________________________________________________________________________________________________________________
 * Rounds an MRMS lat or lon to two decimal places using a midpoint rounding strategy.
 *
 * @param {number} n - The number to round.
 * @returns {string} A string representation of the rounded number.
 */
const MRMSround = (n) => (Math.round((n - 0.005) * 100) / 100 + 0.005).toFixed(3);

/** ____________________________________________________________________________________________________________________________________
 * Sanitizes a string for safe use in an SQL query.
 *
 * @param {string} s - The string to sanitize.
 * @returns {string} - The sanitized string.
 */
const sanitize = (s) => (
  (s || '')
    .replace(/\b(select|insert|update|drop|delete|truncate|create|alter|grant|revoke)\b/ig, '')
    .replace(/'/g, `''`)
); // sanitize

/** ____________________________________________________________________________________________________________________________________
 * Sanitizes a query parameter by converting it to a safe SQL string.
 * Works for both POST and GET.
 *
 * @param {object} req - The request object from Express.js.
 * @param {string} parm - The name of the query parameter to sanitize.
 * @returns {string} A sanitized SQL string.
 */
const safeQuery = (req, parm) => (
  sanitize(req.body[parm] || req.query[parm])
); // safeQuery

/** ____________________________________________________________________________________________________________________________________
 * Convert a comma-separated string to an array of safe SQL-quoted strings.
 *
 * @param {string} s - The comma-separated string to convert.
 * @param {string} [method='toString'] - The method to call on each array element to convert to a string.
 * @returns {string|null} A comma-separated string of safe SQL-quoted strings, or null if input is falsy.
 */
const safeQuotes = (s, method = 'toString') => {
  if (!s) {
    return null;
  }

  return s.split(',').map((s2) => `'${s2[method]().replace(/'/g, `''`)}'`).join(',');
}; // safeQuotes

/** ____________________________________________________________________________________________________________________________________
 * sql-formatter puts comma-separated items on separate lines.
 *
 * wrapText wraps the text at the specified maximum length,
 * while maintaining the current indentation
 * and preserving comma-separated items on the same line.
 *
 * @param {string} text The text to wrap.
 * @param {number} [maxLength=process.stdout.columns] The maximum line length.
 * @returns {string} The wrapped text.
 */
const wrapText = (text, maxLength = process.stdout.columns) => {
  const lines = text.split('\n');

  lines.forEach((line, index) => {
    if (line.length > maxLength) {
      const terms = [];
      let paren = 0;
      if (line.trim()[0] === ')') {
        paren = 1;
      }
      let s = '';
      [...line].forEach((c) => {
        s += c;
        if (c === '(') paren += 1;
        else if (c === ')') paren -= 1;
        else if (c === ',' && paren === 0) {
          terms.push(s);
          s = '';
        }
      });
      terms.push(s);

      const indent = line.match(/ +/)?.[0] || '';
      const ml = maxLength - indent;
      s = '';
      const wrapped = [];
      terms.forEach((term) => {
        if ((s + term).length > ml) {
          wrapped.push(indent + s.trim());
          s = indent;
        }

        s += term;
      });
      wrapped.push(indent + s.trim());
      lines[index] = wrapped.join('\n');
    }
  });

  return lines.join('\n');
}; // wrapText

/** ____________________________________________________________________________________________________________________________________
 * Formats and prints a SQL query to the console.
 *
 * @param {string} sq - The SQL query to format and print.
 * @returns {string} The formatted SQL query.
 */
// eslint-disable-next-line no-unused-vars
const pretty = (sq) => {
  let result;
  try {
    result = format(
      sq,
      {
        language: 'postgresql',
      },
    )
      .replace(/,\s*\n\s*/g, ', ');

    result = wrapText(result);
  } catch (error) {
    // in case sql-formatter bombs
    console.warn(error);
    result = sq;
  }
  console.log(result);
  return result;
}; // pretty

/** ____________________________________________________________________________________________________________________________________
 * Gets the latitude and longitude coordinates of a location using the Google Maps API or the database.
 * (New locations are added to the database.)
 * If `location` is a valid ZIP code, it will be automatically converted to "zip <code>".
 * If `rect` is `true`, also calculates the bounding box (minLat, maxLat, minLon, maxLon) for the location.
 * If `func` is provided, it will be called with the resulting latitude and longitude arrays.
 *
 * @param {Object} res - Express response object.
 * @param {Function} func - Optional callback function to receive the resulting latitude and longitude arrays.
 */
const getLocation = (res, func) => {
  if (+location) {
    location = `zip ${location}`;
  }
  pool.query(
    'select * from weather.addresses where address=$1',
    [location],
    (err, results) => {
      if (err) {
        send(res, err);
      } else if (results.rows.length) {
        debug(`Found ${location}`);
        lats = [results.rows[0].lat];
        lons = [results.rows[0].lon];
        if (rect) {
          minLat = Math.min(results.rows[0].lat1, results.rows[0].lat2);
          maxLat = Math.max(results.rows[0].lat1, results.rows[0].lat2);
          minLon = Math.min(results.rows[0].lon1, results.rows[0].lon2);
          maxLon = Math.max(results.rows[0].lon1, results.rows[0].lon2);
        }
        if (func) {
          func(lats, lons);
        }
      } else {
        console.time(`Looking up ${location}`);
        axios.get(`https://maps.googleapis.com/maps/api/geocode/json?address=${location}&key=${googleAPIKey}`)
          .then(({ data }) => {
            console.timeEnd(`Looking up ${location}`);
            if (err) {
              debug({ trigger: 'Google Maps Geocode', location, err }, res, 400);
            } else {
              try {
                // eslint-disable-next-line prefer-destructuring
                const lat = data.results[0].geometry.location.lat;
                const lon = data.results[0].geometry.location.lng;
                const lat1 = data.results[0].geometry.viewport.northeast.lat;
                const lon1 = data.results[0].geometry.viewport.northeast.lng;
                const lat2 = data.results[0].geometry.viewport.southwest.lat;
                const lon2 = data.results[0].geometry.viewport.southwest.lng;

                debug({
                  location,
                  lat,
                  lon,
                  lat1,
                  lon1,
                  lat2,
                  lon2,
                });

                lats = [lat];
                lons = [lon];

                pool.query(`
                  insert into weather.addresses
                  (address, lat, lon, lat1, lon1, lat2, lon2)
                  values ('${location}', ${lat}, ${lon}, ${lat1}, ${lon1}, ${lat2}, ${lon2})
                `);

                if (rect) {
                  minLat = Math.min(lat1, lat2);
                  maxLat = Math.max(lat1, lat2);
                  minLon = Math.min(lon1, lon2);
                  maxLon = Math.max(lon1, lon2);
                }

                if (func) {
                  func(lats, lons);
                }
              } catch (ee) {
                debug(ee.message);
              }
            }
          });
      }
    },
  );
}; // getLocation

/** ____________________________________________________________________________________________________________________________________
 * Removes the leading whitespace indentation from a string.
 *
 * @param {string} s - The input string.
 * @returns {string} - The string without leading whitespace indentation.
 */
const unindent = (s) => {
  const ind = ' '.repeat(s.search(/[^\s]/) - 1);
  const rep = s.replace(new RegExp(ind, 'g'), '');

  return rep;
}; // unindent

/** ____________________________________________________________________________________________________________________________________
 * Processes a query and sends the results to the client in the requested format: csv, html, or json (default).
 * Saves the query to the "hits" table, along with date, IP, and runtime.  This can be avoided using the "nosave" parameter.
 * Saves the results to the "queries" table for faster retrieval of repeat queries. Deletes any older than 30 days.
 * If the "explain" parameter exists, sends EXPLAIN details rather than executing the query.
 *
 * @function
 * @param {Object} req - The HTTP request object.
 * @param {Object} res - The HTTP response object.
 * @param {string} sq - The SQL query to execute.
 * @returns {undefined}
 */
const sendQuery = (req, res, sq) => {
  // pretty(sq);

  const process = (rows) => {
    if (!rows.length) {
      console.warn('No data found');
      send(res, 'No data found');
      return;
    }

    // prevent duplicate rows. screws up LIMIT unfortunately. hopefully unnecessary
    //   let lastJSON;
    //   rows = rows.filter((row) => lastJSON !== (lastJSON = JSON.stringify(row)));

    let s;
    switch (output ? output.toLowerCase() : 'json') {
      case 'csv':
        s = `${Object.keys(rows[0]).toString()}\n${
          rows.map((r) => Object.keys(r).map((v) => r[v])).join('\n')}`;
        // rows.map(r => Object.values(r).toString()).join('<br>');

        res.set('Content-Type', 'text/csv');
        res.setHeader('Content-disposition', `attachment; filename=${lats}.${lons}.HourlyAverages.csv`);
        res.send(s);
        break;

      case 'html':
        s = `
          <script src="https://aesl.ces.uga.edu/scripts/d3/d3.js"></script>
          <script src="https://aesl.ces.uga.edu/scripts/jquery/jquery.js"></script>
          <script src="https://aesl.ces.uga.edu/scripts/jqlibrary.js"></script>
          <script src="https://aesl.ces.uga.edu/scripts/dbGraph.js"></script>

          <link rel="stylesheet" href="/css/dbGraph.css">
          <link rel="stylesheet" href="/css/weather.css">

          <div id="Graph"></div>

          <table id="Data">
            <thead>
              <tr><th>${Object.keys(rows[0]).join('<th>')}</tr>
            </thead>
            <tbody>
              <tr>${rows.map((r) => `<td>${Object.keys(r).map((v) => r[v]).join('<td>')}`).join('<tr>')}</tr>
            </tbody>
          </table>

          <script src="https://aesl.ces.uga.edu/weatherapp/src/weather.js"></script>
        `;
        res.send(s);
        break;

      default:
        if (req.query.explain) {
          res.json({
            query: sq.slice(8).trim(),
            rows,
          });
        } else if (req.callback) {
          req.callback(rows);
        } else if (testing) {
          send(res, 'SUCCESS');
        } else {
          res.json(rows);
        }
    }
  }; // process

  const qq = sq.replace(/'/g, '');

  pool.query('delete from weather.queries where date < now() - interval \'30 day\'');

  pool.query(`select results from weather.queries where query='${qq}'`, (err, results) => {
    if (err) {
      debug(err);
    } else if (!req.query.explain && results.rowCount) {
      process(results.rows[0].results);
      pool.query(`update weather.queries set date=now() where query='${qq}'`);

      if (!req.query.nosave) {
        const hits = `
          insert into weather.hits
          (date, ip, query, ms)
          values (now(), '${ip}', '${req.url}', 0)
        `;
        pool.query(hits);
      }
    } else {
      const startTime = new Date();

      if (req.query.explain) {
        sq = `explain ${sq}`;
      }

      pool.query(sq, (error, results2) => {
        if (error) {
          debug({ sq, error }, res, 500);
          return;
        }

        if (
          !req.query.explain
          && (
            /averages?/.test(req.originalUrl)
            || (req.query.end && new Date() - new Date(req.query.end) > 86400000)
          )
        ) {
          const jr = JSON.stringify(results2.rows);
          pool.query(`insert into weather.queries (date, url, query, results) values (now(), '${req.originalUrl}', '${qq}', '${jr}')`);
        }

        if (!req.query.nosave) {
          const time = new Date() - startTime;
          const hits = `
            insert into weather.hits (date, ip, query, ms)
            values (now(), '${ip}', '${req.url}', ${time})
          `;
          pool.query(hits);
        }
        process(results2.rows);
      });
    }
  });
}; // sendQuery

const runQuery = (req, res, type, start, end, format2, daily) => {
  const query = (offset) => {
    let byx;
    let byy;
    let rtables = {};
    const latlons = [];

    if (rect) {
      byy = Math.max(0.125, 0.125 * Math.floor(maxLat - minLat));
      byx = Math.max(0.125, 0.125 * Math.floor(maxLon - minLon));

      for (let y = minLat; y <= maxLat; y += byy) {
        for (let x = minLon; x <= maxLon; x += byx) {
          rtables[`weather.${type}${Math.trunc(NLDASlat(y))}_${-Math.trunc(NLDASlon(x))}`] = true;
          latlons.push(`'${+NLDASlat(y)}${+NLDASlon(x)}'`);
        }
      }
      rtables = Object.keys(rtables);
    }

    let sq;
    const cond = where ? ` and (${where})` : '';
    const dateCond = `date::timestamp + interval '${offset} seconds' between '${start}'::timestamp and '${end}'::timestamp`;
    const tables = rect ? rtables
      .map((table) => unindent(`
        select lat as rlat, lon as rlon, *
        from (
          ${years.map((year) => (type === 'ha_' ? `select * from ${table}` : `select * from ${table}_${year}`)).join(' union all ')}
        ) a
        where lat::text || lon in (${latlons}) and
              date::timestamp + interval '${offset} seconds' between '${start}' and '${end}'
              ${cond}
      `))
      .join(' union all\n')
      : lats
        .map((lat, i) => {
          let mainTable = type === 'nldas_hourly_'
            ? years.map(
              (year) => unindent(`
                select ${parms}, precipitation as nldas
                from weather.${type}${Math.trunc(NLDASlat(lat))}_${-Math.trunc(NLDASlon(lons[i]))}_${year}
                where lat=${NLDASlat(lat)} and lon=${NLDASlon(lons[i])} and
                      ${dateCond}
              `),
            ).join(' union all ')
            : unindent(`
              select *
              from weather.${type}${Math.trunc(NLDASlat(lat))}_${-Math.trunc(NLDASlon(lons[i]))}
              where lat=${NLDASlat(lat)} and lon=${NLDASlon(lons[i])} and
                    ${dateCond}
            `);

          if (type === 'nldas_hourly_' && (req.query.predicted === 'true' || options.includes('predicted'))) {
            let maxdate = '';
            const year = new Date().getFullYear();

            if (mainTable) {
              mainTable += ' union all ';
              maxdate = `
                date > (
                  select max(date) from (
                    select date from weather.${type}${Math.trunc(NLDASlat(lat))}_${-Math.trunc(NLDASlon(lons[i]))}_new union all
                    select date from weather.${type}${Math.trunc(NLDASlat(lat))}_${-Math.trunc(NLDASlon(lons[i]))}_${year}
                  ) a
                )
                and
              `;
            }

            mainTable += range(+start.slice(0, 4), +end.slice(0, 4) + 1)
              .map((y) => `
                select ${parms}, precipitation as nldas from (
                  select
                    make_timestamp(${y},
                    extract(month from date)::integer, extract(day from date)::integer, extract(hour from date)::integer, 0, 0) as date,
                    ${parms.slice(1)},
                    precipitation as nldas
                  from weather.ha_${Math.trunc(NLDASlat(lat))}_${-Math.trunc(NLDASlon(lons[i]))}
                ) a
                where
                  lat=${NLDASlat(lat)} and lon=${NLDASlon(lons[i])} and
                  ${maxdate}
                  ${dateCond}
              `).join(' union all ');
          }

          if (mrms && years.length) {
            const mrmsTable = `
              (${years.map((year) => `
                  select * from weather.mrms_${Math.trunc(MRMSround(lat))}_${-Math.trunc(MRMSround(lons[i]))}_${year}
                  where lat = ${MRMSround(lat)} and lon = ${MRMSround(lons[i])} and ${dateCond}
                `).join(' union all ')
}
                union all

                select * from weather.mrms_${Math.trunc(NLDASlat(lat))}_${-Math.trunc(NLDASlon(lons[i]))}_new
                where lat = ${MRMSround(lat)} and lon = ${MRMSround(lons[i])} and ${dateCond}

                union all

                select b.date, lat, lon, precipitation from weather.mrmsmissing a
                left join (${mainTable}) b
                ON a.date = b.date
                where lat=${NLDASlat(lat)} and lon=${NLDASlon(lons[i])}
              ) m
            `;

            // originally select distinct:
            const sq2 = `
              SELECT ${lat} AS rlat, ${lons[i]} AS rlon, *
              FROM (
                SELECT 
                  COALESCE(a.date, b.date) AS date,
                  COALESCE(a.lat, b.lat) AS lat,
                  COALESCE(a.lon, b.lon) AS lon,
                  air_temperature,
                  humidity,
                  relative_humidity,
                  pressure,
                  zonal_wind_speed,
                  meridional_wind_speed,
                  wind_speed,
                  longwave_radiation,
                  convective_precipitation,
                  potential_energy,
                  potential_evaporation,
                  shortwave_radiation,
                  coalesce(b.precipitation, 0) AS precipitation,
                  nldas
                FROM (
                  ${mainTable}
                ) a
                FULL JOIN (
                  SELECT * FROM ${mrmsTable}
                ) b
                ON a.date = b.date
              ) alias1
              WHERE
                ${dateCond}
                ${cond}
            `;

            // pretty(sq2);
            return sq2;
          }
          return `
            select ${lat} as rlat, ${lons[i]} as rlon, *
            from (${mainTable}) a
            where lat=${NLDASlat(lat)} and lon=${NLDASlon(lons[i])} and
                  date::timestamp + interval '${offset} seconds' between '${start}' and '${end}'
                  ${cond}
          `;
        })
        .join(' union all\n');

    // if (req.query.predicted === 'true') {
    //    send(res, tables.replace(/[\n\r]+/g, '<br>')); return;
    // }

    const order = req.query.order
      || `1 ${cols.split(/\s*,\s*/).includes('lat') ? ',lat' : ''} ${cols.split(/\s*,\s*/).includes('lon') ? ',lon' : ''}`;

    if (daily) {
      sq = `
        select to_char(date::timestamp + interval '${offset} seconds', '${format2}') as date,
                ${cols.replace(/\blat\b/, 'rlat as lat').replace(/\blon\b/, 'rlon as lon')}
        from (
          select date as GMT, *
          from (${tables}) tables
        ) a
        group by to_char(date::timestamp + interval '${offset} seconds', '${format2}'), rlat, rlon
        order by ${order}
      `;
    } else {
      let other = '';
      const gy = `
        (extract(year from (date::timestamp + interval '${offset} seconds' - interval '5 months')))::text || '-' ||
        (extract(year from (date::timestamp + interval '${offset} seconds' - interval '5 months')) + 1)::text
        as growingyear, 
      `;

      if (/\bdoy\b/.test(req.query.stats)) {
        other += `extract(doy from date::timestamp + interval '${offset} seconds') as doy, `;
      }

      if (/\bmonth\b/.test(req.query.stats)) {
        other += `extract(month from date::timestamp + interval '${offset} seconds') as month, `;
      }

      if (/\bgrowingyear\b/.test(req.query.stats)) {
        other += gy;
      }

      if (/\byear\b/.test(req.query.stats)) {
        other += `extract(year from date::timestamp + interval '${offset} seconds') as year, `;
      }

      if (req.query.group) {
        other += req.query.group
          .replace(/\bdoy\b/g, `extract(doy from date::timestamp + interval '${offset} seconds') as doy, `)
          .replace(/\bmonth\b/g, `extract(month from date::timestamp + interval '${offset} seconds') as month, `)
          .replace(/\byear\b/g, `extract(year from date::timestamp + interval '${offset} seconds') as year, `)
          .replace(/\bgrowingyear\b/g, gy);
      }

      sq = `
        select
          ${other} to_char(date::timestamp + interval '${offset} seconds', '${format2}') as date,
          ${cols.replace(/\blat\b/, 'rlat as lat').replace(/\blon\b/, 'rlon as lon')}
        from (
          select date as GMT, *
          from (${tables}) tables
        ) a
        order by ${order}
      `;
    }

    if (stats) {
      sq = `
        select ${group ? `${group}, ` : ''} ${stats}
        from (
          ${sq}
        ) alias
        ${group ? `group by ${group}` : ''}
      `;
    }

    if (req.query.gaws) {
      attr = (req.query.attr || '').split(',');
      sq = unindent(`
        select *
        from (
          ${sq}
        ) a
        left join (
          select
            ${attr.includes('air_temperature') ? `
              min_air_temperature as ws_min_air_temperature,
              max_air_temperature as ws_max_air_temperature,
              avg_air_temperature as ws_avg_air_temperature,
            ` : ''}
            ${attr.includes('soil_temperature') ? `
              min_soil_temperature_10cm as ws_min_soil_temperature,
              max_soil_temperature_10cm as ws_max_soil_temperature,
              avg_soil_temperature_10cm as ws_avg_soil_temperature,
            ` : ''}
            ${attr.includes('soil_temperature') ? `
              min_water_temp as ws_min_water_temperature,
              max_water_temp as ws_max_water_temperature,
              avg_water_temp as ws_avg_water_temperature,
            ` : ''}
            ${attr.includes('pressure') ? `
              min_atmospheric_pressure as ws_min_pressure,
              max_atmospheric_pressure as ws_max_pressure,
              avg_atmospheric_pressure as ws_avg_pressure,
            ` : ''}
            ${attr.includes('relative_humidity') ? `
              min_humidity / 100 as ws_min_relative_humidity,
              max_humidity / 100 as ws_max_relative_humidity,
              avg_humidity / 100 as ws_avg_relative_humidity,
            ` : ''}
            ${attr.includes('dewpoint') ? `
              min_dewpoint as ws_min_dewpoint,
              max_dewpoint as ws_max_dewpoint,
              avg_dewpoint as ws_avg_dewpoint,
            ` : ''}
            ${attr.includes('vapor_pressure') ? `
              min_vapor_pressure as ws_min_vapor_pressure,
              max_vapor_pressure as ws_max_vapor_pressure,
              avg_vapor_pressure as ws_avg_vapor_pressure,
            ` : ''}
            date
          from weather.stationdata
          where site=${req.query.gaws}
        ) b
        ON a.date::date = b.date::date
      `);
    }

    if (req.query.limit) {
      sq = `select * from (${sq}) a limit ${req.query.limit} offset ${req.query.offset}`;
    }

    sendQuery(req, res, sq);
  }; // query

  const getTimeZone = () => {
    if (options.includes('gmt') || options.includes('utc')) {
      return query(0);
    }
    pool.query(
      `select * from weather.timezone
       where lat=${NLDASlat(lats[0])} and lon=${NLDASlon(lons[0])}
      `,
      (err, results) => {
        if (err) {
          debug({
            trigger: 'timezone', lat: lats[0], lon: lons[0], err,
          }, res, 400);
          return false;
        }

        if (results.rows.length) {
          return query(results.rows[0].rawoffset);
        }

        console.time('Getting timezone');
        axios.get(
          `https://maps.googleapis.com/maps/api/timezone/json?location=${NLDASlat(lats[0])},${NLDASlon(lons[0])}&timestamp=0&key=${googleAPIKey}`,
        )
          .then(({ data }) => {
            console.timeEnd('Getting timezone');
            console.log(data);
            if (data.status === 'ZERO_RESULTS') { // Google API can't determine timezone for some locations over water, such as (28, -76)
              return query(0);
            }

            pool.query(`
              insert into weather.timezone (lat, lon, dstOffset, rawOffset, timeZoneId, timeZoneName)
              values (${NLDASlat(lats[0])}, ${NLDASlon(lons[0])}, ${data.dstOffset}, ${data.rawOffset}, '${data.timeZoneId}', '${data.timeZoneName}')
            `);

            return query(data.rawOffset);
          })
          .catch((error) => {
            debug(
              {
                trigger: 'Google API timezone',
                lat: lats[0],
                lon: lons[0],
                error,
              },
              res,
              500,
            );
          });

        return false;
      },
    );

    return false;
  }; // getTimeZone

  /**
   * Cleans a string by removing specific characters and forbidden keywords.
   * @param {string} s - The string to clean.
   * @returns {string} The cleaned string or 'ERROR' if forbidden keywords are found.
   */
  const clean = (s) => {
    const t = decodeURI(s)
      .replace(/["()+\-*/<>,= 0-9.]/ig, '')
      .replace(/doy|day|month|year|growingyear|sum|min|max|avg|count|stddev_pop|stddev_samp|variance|var_pop|var_samp|date|as|abs|and|or|not/ig, '')
      .replace(/between|tmp|air_temperature|spfh|humidity|relative_humidity|pres|pressure|ugrd|zonal_wind_speed|wind_speed|vgrd/ig, '')
      .replace(/meridional_wind_speed|dlwrf|longwave_radiation|frain|convective_precipitation|cape|potential_energy|pevap|/ig, '')
      .replace(/potential_evaporation|apcp|precipitation|mrms|dswrf|shortwave_radiation|gdd/ig, '');

    if (t) {
      console.error('*'.repeat(80));
      console.error(t);
      console.error('*'.repeat(80));
      return 'ERROR';
    }
    return s;
  }; // clean

  /**
   * Fixes column names by replacing specific abbreviations with full column names.
   * @param {string} col - The column name to fix.
   * @param {boolean} [alias=false] - Determines if column aliases should be included.
   * @returns {string} The fixed column name with optional aliases.
   */
  const fix = (col, alias) => col.replace(/\btmp\b/i, `air_temperature${alias ? ' as TMP' : ''}`)
    .replace(/\bspfh\b/i, `humidity${alias ? ' as SPFH' : ''}`)
    .replace(/\bpres\b/i, `pressure${alias ? ' as PRES' : ''}`)
    .replace(/\bugrd\b/i, `zonal_wind_speed${alias ? ' as UGRD' : ''}`)
    .replace(/\bvgrd\b/i, `meridional_wind_speed${alias ? ' as VGRD' : ''}`)
    .replace(/\bdlwrf\b/i, `longwave_radiation${alias ? ' as DLWRF' : ''}`)
    .replace(/\bfrain\b/i, `convective_precipitation${alias ? ' as FRAIN' : ''}`)
    .replace(/\bcape\b/i, `potential_energy${alias ? ' as CAPE' : ''}`)
    .replace(/\bpevap\b/i, `potential_evaporation${alias ? ' as PEVAP' : ''}`)
    .replace(/\bapcp\b/i, `precipitation${alias ? ' as APCP' : ''}`)
    .replace(/\bdswrf\b/i, `shortwave_radiation${alias ? ' as DSWRF' : ''}`); // fix

  /**
   * Generates SQL aggregation functions for statistical calculations on a given parameter.
   *
   * @param {string} parm - The parameter for which to generate the statistics.
   * @returns {string} - The SQL aggregation functions for minimum, maximum, and average of the parameter.
   */
  const statistics = (parm) => {
    parm = parm.padEnd(25);
    return `min(${parm}) as min_${parm}, max(${parm}) as max_${parm}, avg(${parm}) as avg_${parm}`;
  }; // statistics

  /**
   * Generates an SQL aggregation function to calculate the sum of a given parameter.
   *
   * @param {string} parm - The parameter for which to generate the sum.
   * @returns {string} - The SQL aggregation function for the sum of the parameter.
   */
  const sum = (parm) => {
    parm = parm.padEnd(25);
    return `sum(${parm}) as ${parm}`;
  }; // sum

  /**
   * Determines the columns to be selected in the database query based on the provided parameters.
   */
  const getColumns = () => {
    if (daily) {
      if (attr) {
        cols = attr.toLowerCase()
          .replace(/,?gdd/g, '') // included automatically if req.query.gddbase
          .split(',')
          .map((col) => {
            if (/^(lat|lon)$/.test(col)) {
              return col;
            }

            if (/mrms|precipitation|radiation|potential/.test(col)) {
              return `sum(${fix(col, false)}) as ${col}`;
            }

            return `min(${fix(col, false)}) as min_${col}, max(${fix(col, false)}) as max_${col}, avg(${fix(col, false)}) as avg_${col}`;
          })
          .join(',');
      } else {
        cols = `
          lat, lon,
          ${sum('precipitation')},
          ${sum('longwave_radiation')},
          ${sum('shortwave_radiation')},
          ${sum('potential_energy')},
          ${sum('potential_evaporation')},
          ${sum('convective_precipitation')},
          ${statistics('air_temperature')},
          ${statistics('humidity')},
          ${statistics('relative_humidity')},
          ${statistics('pressure')},
          ${statistics('zonal_wind_speed')},
          ${statistics('meridional_wind_speed')},
          ${statistics('wind_speed')}
        `;
        console.log(cols);
      }
      const { gddbase } = req.query;
      if (gddbase) {
        const mintemp = req.query.gddmin || gddbase;
        const maxtemp = req.query.gddmax || 999;

        cols += `,
          greatest(0, (
            least(${maxtemp}, max(air_temperature)) + greatest(${mintemp}, least(${maxtemp}, min(air_temperature)))) / 2 - ${gddbase}
          ) as gdd
        `;
      }
    } else {
      cols = attr ? fix(attr, true) : parms.slice(1).join(', ');

      if (/averages|daily/.test(req.url)) {
        cols = cols.replace(', frost', '');
      }
    }
  }; // getColumns

  attr = (req.query.attributes || req.query.attr || '')
    .replace(/(soil_temperature|water_temperature|dewpoint|vapor_pressure),?/g, '')
    .replace(/,$/, '');
  // const year1 = Math.max(+start.slice(0, 4), 2015);
  const year1 = Math.max(+start.slice(0, 4), 2005);
  const year2 = Math.min(+end.slice(0, 4), new Date().getFullYear());
  group = req.query.group;
  where = req.query.where
    ? clean(fix(req.query.where))
      .replace(/month/g, 'extract(month from date)')
    : '';
  stats = req.query.stats
    ? clean(fix(req.query.stats.replace(/[^,]+/g, (s) => `${s} as "${s}"`)))
    : '';

  if (attr && /averages|daily/.test(req.url)) {
    attr = attr.replace(/, *frost/, '').replace(/, *nldas/, '');
  }

  years = range(year1, Math.min(year2 + 1, new Date().getFullYear()));

  if (year2 === new Date().getFullYear()) {
    years.push('new');
  }

  mrms = year2 > 2014 && /hourly|daily/.test(req.url) && !/nomrms/.test(options);

  getColumns();

  if (location) {
    getLocation(res, getTimeZone);
  } else {
    lats = (req.query.lat.toString() || '').split(',');
    lons = (req.query.lon.toString() || '').split(',');
    if (rect && lats.length === 2) {
      minLat = Math.min(...lats);
      maxLat = Math.max(...lats);
      minLon = Math.min(...lons);
      maxLon = Math.max(...lons);
    }
    getTimeZone();
  }
}; // runQuery

const routeHourly = (req = testRequest, res = testResponse) => {
  const start = req.query.start || '2000-01-01';
  const end = req.query.end ? req.query.end + (/:/.test(req.query.end) ? '' : ' 23:59')
    : '2099-12-31 23:59';

  req.url = req.url || 'hourly'; // in case testing

  runQuery(req, res, 'nldas_hourly_', start, end, 'YYYY-MM-DD HH24:MI');
}; // routeHourly

const routeDaily = (req = testRequest, res = testResponse) => {
  const start = req.query.start || '2000-01-01';
  const end = req.query.end ? `${req.query.end} 23:59` : '2099-12-31 23:59';

  req.url = req.url || 'daily'; // in case testing

  runQuery(req, res, 'nldas_hourly_', start, end, 'YYYY-MM-DD', true);
}; // routeDaily

const routeAverages = (req = testRequest, res = testResponse) => {
  let start = req.query.start || '01-01';
  let end = req.query.end ? `${req.query.end} 23:59` : '12-31 23:59';

  req.url = req.url || 'averages'; // in case testing

  if (start.split('-').length === 3) { // drop year
    start = start.slice(start.indexOf('-') + 1);
  }

  if (end.split('-').length === 3) { // drop year
    end = end.slice(end.indexOf('-') + 1);
  }

  runQuery(req, res, 'ha_', `2099-${start}`, `2099-${end}`, 'MM-DD HH24:MI');
}; // routeAverages

const queryJSON = (req, res, sq) => {
  pool.query(
    `${sq}
     limit ${req.query.limit || 100000}
     offset ${req.query.offset || 0}
    `,
    (err, results) => {
      if (err) {
        debug(err, res, 500);
      } else if (testing) {
        send(res, 'SUCCESS');
      } else {
        res.json(results.rows);
      }
    },
  );
}; // queryJSON

const routeGAWeatherStations = (req = testRequest, res = testResponse) => {
  queryJSON(req, res, 'select * from weather.stations order by County');
}; // routeGAWeatherStations

const routeAddresses = (req = testRequest, res = testResponse) => {
  queryJSON(req, res, 'select * from weather.addresses order by address');
}; // routeAddresses

const routeIndexes = (req = testRequest, res = testResponse) => {
  queryJSON(req, res, 'select * from pg_indexes where tablename not like \'pg%\' order by indexname');
}; // routeIndexes

const routeTables = (req = testRequest, res = testResponse) => {
  queryJSON(req, res, `
    select table_name as table, reltuples as rows from (
      select * from information_schema.tables
      where table_schema='weather'
    ) a
    left join pg_class b
    ON a.table_name = b.relname
    where reltuples > 0
    order by table_name
  `);
}; // routeTables

const routeCountTablesRows = (req = testRequest, res = testResponse) => {
  queryJSON(req, res, `
    select
      count(*) as tables,
      sum(reltuples) as rows
    from (
      select * from information_schema.tables
      where table_schema='weather'
    ) a
    left join pg_class b
    ON a.table_name = b.relname
    where reltuples > 0    
  `);
}; // routeCountTablesRows

const routeCountIndexes = (req = testRequest, res = testResponse) => {
  queryJSON(req, res, `
    select count(*) as indexes
    from pg_indexes
    where schemaname = 'weather'
  `);
}; // routeCountIndexes

const routeDatabasesize = (req = testRequest, res = testResponse) => {
  queryJSON(req, res, `
    select pg_size_pretty(pg_database_size('postgres')) as size
  `);
}; // routeDatabasesize

const routeHits = (req = testRequest, res = testResponse) => {
  queryJSON(
    req,
    res,
    `
     select * from weather.hits
     where
       query not like '%explain%' and query not like '%nvm%' and
       (date > current_date - 1 or (ip <> '::ffff:172.18.186.142' and query not like '%25172.18.186%25'))
     order by date desc
    `,
  );
}; // routeHits

const routeMvm = (req = testRequest, res = testResponse) => {
  const sq = `
    select a.sum - b.sum as delta,
           a.lat as alat, a.lon as alon, a.sum as asum,
           b.lat as blat, b.lon as blon, b.sum as bsum
    from weather.mrms_${req.query.lat}_${-req.query.lon}_2019_annual a
    left join weather.mrms_${req.query.lat}_${-req.query.lon}_2019_annual b
    ON (a.lat between b.lat + 0.01 and b.lat + 0.011 and
        a.lon = b.lon
       ) or (
        a.lat = b.lat and
        a.lon between b.lon + 0.01 and b.lon + 0.011
       )
    where
      b.lat is not null and
      abs(a.sum - b.sum) > ${req.query.num}
    order by 1 desc
  `;

  pool.query(sq, (err, results) => {
    if (err) {
      send(res, `ERROR:<br>${sq.replace(/\n/g, '<br>').replace(/ /g, '&nbsp;')}`);
    } else if (testing) {
      send(res, 'SUCCESS');
    } else {
      send(res, JSON.stringify(results.rows));
    }
  });
}; // routeMvm

const routeNvm = (req = testRequest, res = testResponse) => {
  let mlat;
  let mlon;
  let nlat;
  let nlon;

  const NVMprocess = () => {
    const sq1 = `select b.year as "Year", coalesce(round(a.total), 0) as "MRMS<br>precipitation", round(b.totalz) as "NLDAS<br>precipitation" from (
                select to_char(date, 'yyyy') as year, sum(precipitation) as total from (
                  select * from weather.mrms_${Math.trunc(mlat)}_${Math.trunc(-mlon)}_2015
                  where abs(lat - ${mlat}) < 0.001 and abs(lon  - ${mlon}) < 0.001 and date>'2015-05-06'
                  union all
                  select * from weather.mrms_${Math.trunc(mlat)}_${Math.trunc(-mlon)}_2016
                  where abs(lat - ${mlat}) < 0.001 and abs(lon  - ${mlon}) < 0.001
                  union all
                  select * from weather.mrms_${Math.trunc(mlat)}_${Math.trunc(-mlon)}_2017
                  where abs(lat - ${mlat}) < 0.001 and abs(lon  - ${mlon}) < 0.001
                  union all
                  select * from weather.mrms_${Math.trunc(mlat)}_${Math.trunc(-mlon)}_2018
                  where abs(lat - ${mlat}) < 0.001 and abs(lon  - ${mlon}) < 0.001
                  union all
                  select * from weather.mrms_${Math.trunc(mlat)}_${Math.trunc(-mlon)}_2019
                  where abs(lat - ${mlat}) < 0.001 and abs(lon  - ${mlon}) < 0.001
                ) alias
                group by year
              ) a
              full join (
                select to_char(date, 'yyyy') as year, sum(precipitation) as totalz from (
                  select * from weather.nldas_hourly_${Math.trunc(nlat)}_${Math.trunc(-nlon)}_2015
                  where lat = ${nlat} and lon = ${nlon} and date>'2015-05-06'
                  union all
                  select * from weather.nldas_hourly_${Math.trunc(nlat)}_${Math.trunc(-nlon)}_2016
                  where lat = ${nlat} and lon = ${nlon}
                  union all
                  select * from weather.nldas_hourly_${Math.trunc(nlat)}_${Math.trunc(-nlon)}_2017
                  where lat = ${nlat} and lon = ${nlon}
                  union all
                  select * from weather.nldas_hourly_${Math.trunc(nlat)}_${Math.trunc(-nlon)}_2018
                  where lat = ${nlat} and lon = ${nlon}
                  union all
                  select * from weather.nldas_hourly_${Math.trunc(nlat)}_${Math.trunc(-nlon)}_2019
                  where lat = ${nlat} and lon = ${nlon}
                ) alias
                group by year
              ) b
              ON a.year = b.year
              order by 1
             `;

    let sq2 = `select b.month as "Month", coalesce(round(a.total), 0) as "MRMS<br>precipitation", round(b.totalz) as "NLDAS<br>precipitation" from (
                select to_char(date, 'yyyy-mm') as month, sum(precipitation) as total from (
                  select * from weather.mrms_${Math.trunc(mlat)}_${Math.trunc(-mlon)}_2015
                  where abs(lat - ${mlat}) < 0.001 and abs(lon  - ${mlon}) < 0.001 and date>'2015-05-06'
                  union all
                  select * from weather.mrms_${Math.trunc(mlat)}_${Math.trunc(-mlon)}_2016
                  where abs(lat - ${mlat}) < 0.001 and abs(lon  - ${mlon}) < 0.001
                  union all
                  select * from weather.mrms_${Math.trunc(mlat)}_${Math.trunc(-mlon)}_2017
                  where abs(lat - ${mlat}) < 0.001 and abs(lon  - ${mlon}) < 0.001
                  union all
                  select * from weather.mrms_${Math.trunc(mlat)}_${Math.trunc(-mlon)}_2018
                  where abs(lat - ${mlat}) < 0.001 and abs(lon  - ${mlon}) < 0.001
                  union all
                  select * from weather.mrms_${Math.trunc(mlat)}_${Math.trunc(-mlon)}_2019
                  where abs(lat - ${mlat}) < 0.001 and abs(lon  - ${mlon}) < 0.001
                ) alias
                group by month
              ) a
              full join (
                select to_char(date, 'yyyy-mm') as month, sum(precipitation) as totalz from (
                  select * from weather.nldas_hourly_${Math.trunc(nlat)}_${Math.trunc(-nlon)}_2015
                  where lat = ${nlat} and lon = ${nlon} and date>'2015-05-06'
                  union all
                  select * from weather.nldas_hourly_${Math.trunc(nlat)}_${Math.trunc(-nlon)}_2016
                  where lat = ${nlat} and lon = ${nlon}
                  union all
                  select * from weather.nldas_hourly_${Math.trunc(nlat)}_${Math.trunc(-nlon)}_2017
                  where lat = ${nlat} and lon = ${nlon}
                  union all
                  select * from weather.nldas_hourly_${Math.trunc(nlat)}_${Math.trunc(-nlon)}_2018
                  where lat = ${nlat} and lon = ${nlon}
                  union all
                  select * from weather.nldas_hourly_${Math.trunc(nlat)}_${Math.trunc(-nlon)}_2019
                  where lat = ${nlat} and lon = ${nlon}
                ) alias
                group by month
              ) b
              ON a.month = b.month
              order by 1
             `;

    pool.query(sq1, (err, results) => {
      const data = (results2) => {
        const rows = results2.rows.map((r) => {
          const m = r['MRMS<br>precipitation'];
          const n = r['NLDAS<br>precipitation'];
          const rpd = Math.round((Math.abs(m - n) / ((m + n) / 2)) * 100) || 0;

          let style = '';
          if (Math.abs(m - n) > 50.8) {
            if (rpd > 50) {
              style = 'background: red; color: white; font-weight: bold;';
            } else if (rpd > 35) {
              style = 'background: orange;';
            } else if (rpd > 20) {
              style = 'background: yellow;';
            }
          }

          return `<td>${Object.keys(r).map((v) => r[v]).join('<td>')}<td style="${style}">${rpd}`;
        }).join('<tr>');

        return `
          <table id="Data">
            <thead>
              <tr><th>${Object.keys(results2.rows[0]).join('<th>')}<th>RPD</tr>
            </thead>
            <tbody>
              <tr>
                ${rows}
              </tr>
            </tbody>
          </table>
        `;
      }; // data

      let s = `
        <link rel="stylesheet" href="/css/weather.css">
        <style>th {width: 10em;}</style>
      `;

      if (err) {
        send(res, `ERROR:<br>${sq1.replace(/\n/g, '<br>').replace(/ /g, '&nbsp;')}`);
        return;
      }
      s += `${data(results)}<hr>`;

      if (req.query.explain) {
        sq2 = `explain ${sq2}`;
      }

      pool.query(sq2, (e, results2) => {
        if (e) {
          send(res, `ERROR:<br>${sq2.replace(/\n/g, '<br>').replace(/ /g, '&nbsp;')}`);
        } else if (testing) {
          send(res, 'SUCCESS');
        } else {
          s += data(results2);
          send(res, s);
        }
      });
    });
  }; // NVMprocess

  if (location) {
    getLocation(res, (rlats, rlons) => {
      mlat = MRMSround(rlats);
      mlon = MRMSround(rlons);
      nlat = NLDASlat(rlats);
      nlon = NLDASlon(rlons);

      NVMprocess();
    });
  } else {
    mlat = MRMSround(req.query.lat);
    mlon = MRMSround(req.query.lon);
    nlat = NLDASlat(req.query.lat);
    nlon = NLDASlon(req.query.lon);

    NVMprocess();
  }
}; // routeNvm

const routeNvm2 = (req = testRequest, res = testResponse) => {
  const lat = Math.round(req.query.lat);
  const lon = Math.round(req.query.lon);
  const { year } = req.query;

  try {
    const runNvmQuery = () => {
      const sq = `select n.date, n.lat, n.lon, nldas, coalesce(mrms, 0) as mrms from (
                  select date, lat, lon, precipitation as nldas from weather.nldas_hourly_${lat}_${-lon}_${year}
                  where (lat, lon) in (
                    (${lat}.125, ${lon}.875), 
                    (${lat}.125, ${lon}.625),
                    (${lat}.125, ${lon}.375),
                    (${lat}.125, ${lon}.125),
                    (${lat}.375, ${lon}.875),
                    (${lat}.375, ${lon}.625),
                    (${lat}.375, ${lon}.375),
                    (${lat}.375, ${lon}.125),
                    (${lat}.625, ${lon}.875),
                    (${lat}.625, ${lon}.625),
                    (${lat}.625, ${lon}.375),
                    (${lat}.625, ${lon}.125),
                    (${lat}.875, ${lon}.875),
                    (${lat}.875, ${lon}.625),
                    (${lat}.875, ${lon}.375),
                    (${lat}.875, ${lon}.125)
                  )
                ) n
                left join (
                  select date, lat, lon, precipitation as mrms from weather.mrms_${lat}_${-lon}_${year}
                  where (lat, lon) in (
                    (${lat}.125, ${lon}.875),
                    (${lat}.125, ${lon}.625),
                    (${lat}.125, ${lon}.375),
                    (${lat}.125, ${lon}.125),
                    (${lat}.375, ${lon}.875),
                    (${lat}.375, ${lon}.625),
                    (${lat}.375, ${lon}.375),
                    (${lat}.375, ${lon}.125),
                    (${lat}.625, ${lon}.875),
                    (${lat}.625, ${lon}.625),
                    (${lat}.625, ${lon}.375),
                    (${lat}.625, ${lon}.125),
                    (${lat}.875, ${lon}.875),
                    (${lat}.875, ${lon}.625),
                    (${lat}.875, ${lon}.375),
                    (${lat}.875, ${lon}.125)
                  )
                ) m
                using (date, lat, lon)
                order by 1, 2, 3
              `;

      pool.query(
        `select lat, lon, extract(month from date) as month, sum(nldas) as nldas, sum(mrms) as mrms from (
           ${sq}
         ) alias
         group by lat, lon, month
         order by 1, 2, 3
        `,
        (err, results) => {
          if (err) {
            send(res, err);
          } else {
            try {
              let s = `
                <link rel="stylesheet" href="css/weather.css">
                <link rel="stylesheet" href="//aesl.ces.uga.edu/weatherapp/src/nvm2.css">
                <script src="https://aesl.ces.uga.edu/scripts/jquery/jquery.js"></script>
                <script src="https://aesl.ces.uga.edu/scripts/jqLibrary.js"></script>
                <script src="https://aesl.ces.uga.edu/weatherapp/src/nvm2.js"></script>
                <script>let monthly = ${JSON.stringify(results.rows)};</script>
                <div id="Data"></div>
               `;

              pool.query(
                `select
                   to_char(date, 'yyyy-mm-dd HH:00') as "Date", lat as "Lat", lon as "Lon",
                   round(nldas) as "NLDAS",
                   round(mrms)  as "MRMS",
                   round(mrms - nldas) as "&Delta;"
                 from (
                   ${sq}
                 ) alias
                 where abs(mrms - nldas) > 13
                `,
                (e, results2) => {
                  if (e || !results2) {
                    send(res, e);
                  } else {
                    try {
                      if (results2.rowCount) {
                        s += `
                          <hr>
                          <table id="Flags">
                            <thead>
                              <tr><th>${Object.keys(results2.rows[0]).join('<th>')}
                            </thead>
                            <tbody>
                              <tr>
                                ${results2.rows.map((r) => Object.keys(r).map((v) => `<td>${r[v]}`).join('')).join('<tr>')}
                              </tr>
                            </tbody>
                          </table>
                          <hr>
                        `;
                      }

                      pool.query(`
                        insert into weather.nvm2 (lat, lon, year, data)
                        values (${lat}, ${lon}, ${year}, '${s.replace(/ /g, ' ').trim()}')
                      `);

                      send(res, s);
                    } catch (error) {
                      send(res, error.message);
                    }
                  }
                },
              );
            } catch (ee) {
              send(res, ee.message);
            }
          }
        },
      );
    }; // runNvmQuery

    pool.query(
      `select data from weather.nvm2 where lat = ${lat} and lon = ${lon} and year = ${year}`,
      (err, results) => {
        if (err) {
          send(res, err);
        } else if (testing) {
          send(res, 'SUCCESS');
        } else if (results.rowCount) {
          send(res, results.rows[0].data);
        } else {
          runNvmQuery();
        }
      },
    );
  } catch (ee) {
    send(res, ee.message);
  }
}; // routeNvm2

const routeNvm2Data = (req, res = testResponse) => {
  pool.query(
    'select distinct lat, lon, year from weather.nvm2',
    (err, results) => {
      if (err) {
        send(res, 'ERROR');
      } else if (testing) {
        send(res, 'SUCCESS');
      } else {
        send(res, JSON.stringify(results.rows));
      }
    },
  );
}; // routeNvm2Data

const routeNvm2Update = (req, res) => {
  const sq = `
    update weather.nvm2
    set red = ${req.query.red},
        orange = ${req.query.orange},
        mismatch = ${req.query.mismatch},
        delta = ${req.query.delta},
        diff = ${req.query.diff},
        mvm = ${req.query.mvm || false},
        mrms = ${req.query.mrms || 'null'}
    where lat = ${req.query.lat} and lon = ${req.query.lon}
  `;

  pool.query(sq);
  send(res, sq);
}; // routeNvm2Update

const routeNvm2Query = (req = testRequest, res = testResponse) => {
  try {
    const sq = `select lat, lon from weather.nvm2
              where ${req.query.condition.replace(/select|insert|update|drop|delete/ig, '')}
              order by lat, lon
             `;

    pool.query(
      sq,
      (err, results) => {
        if (err) {
          send(res, err);
        } else if (testing) {
          send(res, 'SUCCESS');
        } else if (results.rowCount) {
          send(res, JSON.stringify(results.rows));
        }
      },
    );
  } catch (ee) {
    console.error(ee.message);
  }
}; // routeNvm2Query

const routeRosetta = (req = testRequest, res = testResponse) => {
  axios.post(
    'https://www.handbook60.org/api/v1/rosetta/1',
    {
      soildata: req.body.soildata,
    },
  ).then((data) => {
    send(res, data.data);
  });
}; // routeRosetta

const routeWatershed = (req = testRequest, res = testResponse) => {
  const query = (sq) => {
    // pretty(sq);
    pool.query(
      sq,
      (error, results) => {
        if (error) {
          debug({ sq, error }, res, 500);
        } else if (results.rows.length) {
          send(res, results.rows.map((row) => {
            delete row.geometry;
            return row;
          }));
        } else {
          send(res, {});
        }
      },
    );
  }; // query

  let attributes = req.query.attributes?.split(',')
    .map((s) => (
      s.trim()
        .replace(/^name$/, 'huc12.name')
        .replace(/huc(\d+)name/, (_, s2) => `huc${s2}.name as huc${s2}name`)
        .replace(/polygon/i, '(ST_AsGeoJSON(ST_Multi(geometry))::jsonb->\'coordinates\') as polygonarray,ST_AsText(geometry) as polygon')
    ));

  const latLon = () => {
    query(
      `
        SELECT ${attributes}
        FROM huc.huc12
        LEFT JOIN huc.huc10 ON left(huc12.huc12, 10) = huc10.huc10
        LEFT JOIN huc.huc6  ON left(huc12.huc12, 6)  = huc6.huc6
        LEFT JOIN huc.huc8  ON left(huc12.huc12, 8)  = huc8.huc8
        LEFT JOIN huc.huc4  ON left(huc12.huc12, 4)  = huc4.huc4
        LEFT JOIN huc.huc2  ON left(huc12.huc12, 2)  = huc2.huc2
        WHERE ST_Contains(geometry, ST_GeomFromText('POINT(${lons[0]} ${lats[0]})'))
      `,
    );
  }; // latLon

  const { polygon } = req.query;

  const { state } = req.query;

  if (!attributes) {
    if (polygon === 'true') {
      attributes = `
        huc12, huc12.name,
        huc10, huc10.name as huc10name,
        huc8, huc8.name as huc8name,
        huc6, huc6.name as huc6name,
        huc4, huc4.name as huc4name,
        huc2, huc2.name as huc2name,
        tnmid, metasourceid, sourcedatadesc, sourceoriginator, sourcefeatureid,
        loaddate, referencegnis_ids, areaacres, areasqkm, states, hutype, humod,
        tohuc, noncontributingareaacres, noncontributingareasqkm, globalid,
        shape_Length, shape_Area,
        (ST_AsGeoJSON(ST_Multi(geometry))::jsonb->'coordinates') as polygonarray,
        ST_AsText(geometry) as polygon
      `;
    } else {
      attributes = `
        huc12, huc12.name,
        huc10, huc10.name as huc10name,
        huc8, huc8.name as huc8name,
        huc6, huc6.name as huc6name,
        huc4, huc4.name as huc4name,
        huc2, huc2.name as huc2name,
        tnmid, metasourceid, sourcedatadesc, sourceoriginator, sourcefeatureid,
        loaddate, referencegnis_ids, areaacres, areasqkm, states, hutype, humod,
        tohuc, noncontributingareaacres, noncontributingareasqkm, globalid,
        shape_Length, shape_Area
      `;
    }
  }

  const { huc } = req.query;

  if (state) {
    query(
      `
        SELECT ${attributes}
        FROM huc.huc12
        LEFT JOIN huc.huc10 ON left(huc12.huc12, 10) = huc10.huc10
        LEFT JOIN huc.huc6  ON left(huc12.huc12, 6)  = huc6.huc6
        LEFT JOIN huc.huc8  ON left(huc12.huc12, 8)  = huc8.huc8
        LEFT JOIN huc.huc4  ON left(huc12.huc12, 4)  = huc4.huc4
        LEFT JOIN huc.huc2  ON left(huc12.huc12, 2)  = huc2.huc2
        WHERE states like '%${state.toUpperCase()}%'
      `,
    );
  } else if (location) {
    getLocation(res, latLon);
  } else if (huc) {
    query(
      `
        SELECT ${attributes}
        FROM huc.huc12
        LEFT JOIN huc.huc10 ON left(huc12.huc12, 10) = huc10.huc10
        LEFT JOIN huc.huc6  ON left(huc12.huc12, 6)  = huc6.huc6
        LEFT JOIN huc.huc8  ON left(huc12.huc12, 8)  = huc8.huc8
        LEFT JOIN huc.huc4  ON left(huc12.huc12, 4)  = huc4.huc4
        LEFT JOIN huc.huc2  ON left(huc12.huc12, 2)  = huc2.huc2
        WHERE huc12 like '${huc}%'
      `,
    );
  } else {
    lats = (req.query.lat || '').split(',');
    lons = (req.query.lon || '').split(',');
    minLat = Math.min(...lats);
    maxLat = Math.max(...lats);
    minLon = Math.min(...lons);
    maxLon = Math.max(...lons);
    latLon();
  }
}; // routeWatershed

const routeMLRA = (req = testRequest, res = testResponse) => {
  const polygon = safeQuery(req, 'polygon');
  const mlra = safeQuery(req, 'mlra');

  let attributes = req.query.attributes?.split(',')
    .map((s) => (
      s.trim().replace(/polygon/i, '(ST_AsGeoJSON(ST_Multi(geometry))::jsonb->\'coordinates\') as polygonarray,ST_AsText(geometry) as polygon')
    ));

  const query = (sq) => {
    // pretty(sq);
    pool.query(
      sq,
      (err, results) => {
        if (err) {
          debug(err, res, 500);
        } else if (results.rows.length) {
          send(res, results.rows.map((row) => {
            delete row.geometry;
            return row;
          }));
        } else {
          send(res, {});
        }
      },
    );
  }; // query

  const latLon = () => {
    if (mlra) {
      query(`
        SELECT
          b.name,b.mlrarsym,b.lrrsym,b.lrrname,
          STRING_AGG(DISTINCT county || ' County' || ' ' || state, ', ') as counties,
          STRING_AGG(DISTINCT state, ', ') as states,
          STRING_AGG(DISTINCT state_code,', ') as state_codes,
          STRING_AGG(DISTINCT countyfips, ', ') as countyfips,
          STRING_AGG(DISTINCT statefips, ', ') as statefips
          ${polygon ? ', polygon' : ''}
        FROM counties a
        RIGHT JOIN (
          SELECT *, ST_AsText(geometry) as polygon
          FROM mlra.mlra
          WHERE mlrarsym = '${mlra}'
        ) b
        ON ST_Intersects(ST_SetSRID(b.geometry, 4269), a.geometry)
        GROUP BY b.name,b.mlrarsym,b.lrrsym,b.lrrname ${polygon ? ', polygon' : ''}
      `);
    } else {
      query(
        `
          SELECT distinct ${attributes}
          FROM mlra.mlra
          WHERE ST_Contains(geometry, ST_GeomFromText('POINT(${lons[0]} ${lats[0]})'))
        `,
      );
    }
  }; // latLon

  if (!attributes) {
    if (polygon === 'true') {
      attributes = `
        name, mlrarsym, lrrsym, lrrname,
        (ST_AsGeoJSON(ST_Multi(geometry))::jsonb->'coordinates') as polygonarray,
        ST_AsText(geometry) as polygon
      `;
    } else {
      attributes = 'name, mlrarsym, lrrsym, lrrname';
    }
  }

  if (location) {
    getLocation(res, latLon);
  } else {
    lats = (req.query.lat || '').split(',');
    lons = (req.query.lon || '').split(',');
    minLat = Math.min(...lats);
    maxLat = Math.max(...lats);
    minLon = Math.min(...lons);
    maxLon = Math.max(...lons);
    latLon();
  }
}; // routeMLRA

const routeCounty = (req = testRequest, res = testResponse) => {
  const query = (sq) => {
    // pretty(sq);
    pool.query(
      sq,
      (err, results) => {
        if (err) {
          debug(err, res, 500);
        } else if (results.rows.length) {
          send(res, results.rows.map((row) => {
            delete row.geometry;
            return row;
          }));
        } else {
          send(res, {});
        }
      },
    );
  }; // query

  let attributes = req.query.attributes?.split(',')
    .map((s) => (
      s.trim()
        .replace(/polygon/i, '(ST_AsGeoJSON(ST_Multi(geometry))::jsonb->\'coordinates\') as polygonarray,ST_AsText(geometry) as polygon')
    ));

  const latLon = () => {
    query(
      `
        SELECT distinct ${attributes}
        FROM counties
        WHERE ST_Contains(geometry::geometry, ST_Transform(ST_SetSRID(ST_GeomFromText('POINT(${lons[0]} ${lats[0]})'), 4326), 4269))
      `,
    );
  }; // latLon

  const { polygon } = req.query;

  if (!attributes) {
    if (polygon === 'true') {
      attributes = `
        county, state, state_code, countyfips, statefips,
        (ST_AsGeoJSON(ST_Multi(geometry))::jsonb->'coordinates') as polygonarray,
        ST_AsText(geometry) as polygon
      `;
    } else {
      attributes = 'county, state, state_code, countyfips, statefips';
    }
  }

  if (location) {
    getLocation(res, latLon);
  } else {
    lats = (req.query.lat || '').split(',');
    lons = (req.query.lon || '').split(',');
    minLat = Math.min(...lats);
    maxLat = Math.max(...lats);
    minLon = Math.min(...lons);
    maxLon = Math.max(...lons);
    latLon();
  }
}; // routeCounty

const routeCountySpecies = (req = testRequest, res = testResponse) => {
  const county = req.query.county || '%';
  const { state } = req.query;

  const sq = `
    SELECT DISTINCT symbol
    FROM countyspecies
    WHERE county ILIKE '${county}' and state ILIKE '${state}'
    ORDER by symbol
  `;

  pool.query(
    sq,
    (err, results) => {
      if (err) {
        debug(err, res, 500);
      } else {
        send(res, results.rows.map((row) => row.symbol));
      }
    },
  );
}; // routeCountySpecies

const routeMlraSpecies = (req = testRequest, res = testResponse) => {
  // from Access database
  const { mlra } = req.query;

  const sq = `
    SELECT distinct * FROM (
      SELECT plant_symbol, mlra
      FROM mlra_species
    ) a
    INNER JOIN plants b
    ON plant_symbol=symbol
    INNER JOIN plants2 c
    ON plant_symbol=c.symbol
    LEFT JOIN plantfamily d
    ON family=d.family_name
    WHERE mlra='${mlra}';
  `;

  // pretty(sq);

  pool.query(
    sq,
    (err, results) => {
      if (err) {
        debug(err, res, 500);
      } else {
        send(res, results.rows);
      }
    },
  );
}; // routeMlraSpecies

const routeMlraSpecies2 = (req = testRequest, res = testResponse) => {
  // from Access database
  const { mlra } = req.query;

  const sq = `
    SELECT distinct * FROM (
      SELECT plant_symbol, mlra
      FROM mlra_species
    ) a
    INNER JOIN plants3 b
    ON plant_symbol=symbol
    LEFT JOIN plantfamily d
    ON value=d.family_name
    WHERE mlra='${mlra}'
    ORDER BY symbol;
  `;

  // pretty(sq);

  pool.query(
    sq,
    (err, results) => {
      if (err) {
        debug(err, res, 500);
      } else {
        send(res, results.rows);
      }
    },
  );
}; // routeMlraSpecies2

const routeMLRAErrors = (req, res = testResponse) => {
  const sq = `
    select * from (
      select distinct mlrarsym as newmlra from mlra.mlra
    ) a
    full join (
      select distinct mlra as oldmlra from mlra_lrr
    ) b
    ON newmlra=oldmlra
    where newmlra is null or oldmlra is null
  `;

  pool.query(
    sq,
    (err, results) => {
      if (err) {
        debug(err, res, 500);
      } else {
        send(res, results.rows);
      }
    },
  );
}; // routeMLRAErrors

const routePlants = (req = testRequest, res = testResponse) => {
  const symbols = safeQuotes(req.query.symbol, 'toLowerCase');

  if (!symbols) {
    debug({ error: 'symbol required' }, res, 400);
    return;
  }

  const sq = `
    SELECT *
    FROM plants
    WHERE LOWER(symbol) IN (${symbols})
  `;

  // pretty(sq);

  pool.query(
    sq,
    (err, results) => {
      if (err) {
        debug(err, res, 500);
      } else {
        send(res, results.rows);
      }
    },
  );
}; // routePlants

const routePlants2 = (req, res = testResponse) => {
  const sq = `SELECT * FROM plants2 WHERE alepth_ind is not null`;

  // pretty(sq);

  pool.query(
    sq,
    (err, results) => {
      if (err) {
        debug(err, res, 500);
      } else {
        send(res, results.rows);
      }
    },
  );
}; // routePlants2

const simpleQuery = (sq, parameters, res, hideUnused) => {
  pool.query(
    sq,
    parameters,
    (err, results) => {
      if (hideUnused) {
        const used = new Set();
        results.rows.forEach((row) => {
          Object.keys(row).filter((key) => row[key] !== null && row[key] !== '').forEach((key) => used.add(key));
        });
        results.rows.forEach((row) => {
          Object.keys(row).filter((key) => !used.has(key)).forEach((key) => delete row[key]);
        });
      }

      if (err) {
        debug(err, res, 500);
      } else if (results.rows.length) {
        send(res, results.rows);
      } else {
        send(res, {});
      }
    },
  );
}; // simpleQuery

const routeVegspecStructure = (req = testRequest, res = testResponse) => {
  const { table } = req.query;

  const sq = `
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE ${table ? `table_name = $1 AND ` : ''}
      table_schema = 'plants3'
    ORDER BY table_name, ordinal_position;
  `;

  simpleQuery(sq, table ? [table] : [], res);
}; // routeVegspecStructure

const routeMissingCultivars = async (req, res) => {
  const { state } = req.query;

  const results = await pool.query(`
    SELECT DISTINCT * FROM (
      SELECT
        COALESCE(a.plant_symbol, b.plant_symbol) AS symbol,
        b.cultivar_name AS cultivar,
        cultivars AS "known cultivars", state
      FROM (
        SELECT a.*, b.plant_symbol
        FROM (
          SELECT plant_master_id, ARRAY_AGG(cultivar_name ORDER BY cultivar_name) AS cultivars
          FROM plants3.plant_growth_requirements
          GROUP BY plant_master_id
        ) a
        JOIN plants3.plant_master_tbl b
        USING (plant_master_id)
        WHERE plant_symbol IN (
          SELECT plant_symbol FROM plants3.states
          ${state ? ' WHERE state = $1' : ''}
        )
      ) a
      FULL OUTER JOIN (
        SELECT COALESCE(cultivar_name, '') AS cultivar_name, plant_symbol, state
        FROM plants3.states
        ${state ? ' WHERE state = $1' : ''}
      ) b
      ON a.plant_symbol = b.plant_symbol
      ORDER BY a.plant_symbol
    ) a
    WHERE
      not cultivar = ANY("known cultivars") OR
      (cultivar > '' AND "known cultivars" IS NULL)
    ORDER BY state, symbol, cultivar
  `, state ? [state] : undefined);

  send(
    res,
    results.rows.map((row) => {
      row.cultivar = row.cultivar || `<em style="color: gray">${row.cultivar || 'common'}</em>`;
      row.symbol = `<a target="_blank" href="https://plants.sc.egov.usda.gov/home/plantProfile?symbol=${row.symbol}">${row.symbol}</a>`;
      row['known cultivars'] = row['known cultivars']?.filter((c) => c).join(', ');
      return row;
    }),
    { rowspan: true },
  );
}; // routeMissingCultivars

const routeVegspecRecords = (req = testRequest, res = testResponse) => {
  // https://stackoverflow.com/a/38684225/3903374 and Chat-GPT
  const sq = `
    WITH table_stats AS (
      SELECT
        table_name,
        table_schema,
        (xpath('/row/cnt/text()', xml_count))[1]::text::int as row_count
      FROM (
        SELECT
          table_name,
          table_schema,
          query_to_xml(
            format('select count(*) as cnt from %I.%I', table_schema, table_name),
            false, true, ''
          ) as xml_count
        FROM information_schema.tables
        WHERE table_schema = 'plants3'
      ) t
    )
    
    SELECT
      ts.table_name as "table",
      ts.row_count as "rows",
      pg_total_relation_size(format('%I.%I', ts.table_schema, ts.table_name)) as size,
      pg_size_pretty(pg_total_relation_size(format('%I.%I', ts.table_schema, ts.table_name))) AS prettysize
    FROM table_stats ts;
  `;

  simpleQuery(sq, [], res);
}; // routeVegspecRecords

const routePlantsEmptyColumns = async (req = testRequest, res = testResponse) => {
  if (!req.query.generate) {
    const empty = {
      // eslint-disable-next-line max-len
      plant_conservation_status_qualifier: [], plant_image_library: ['plant_image_library_id', 'plant_image_id', 'stream_id', 'last_change_date', 'last_changed_by', 'creation_date', 'created_by', 'active_record_ind'], plant_reserved_symbol_tbl: ['plant_family', 'plant_family_symbol', 'plant_family_id', 'subvariety', 'bauthor_data_source_id', 'tauthor_data_source_id', 'qauthor_data_source_id', 'ssauthor_data_source_id', 'fauthor_data_source_id', 'plant_category', 'plant_category_id', 'hybrid_parent', 'hybrid_parent1', 'hybrid_parent2', 'hybrid_parent3', 'suffix', 'svauthor', 'svauthor_id'], plant_conservation_status: [], county_gen2_project_webmercator: ['objectid', 'shape', 'name', 'state_name', 'fips', 'st'], generated_symbols_with_authorship: [], plant_noxious_status: [], d_plant_location_reference_subject: ['plant_location_reference_subject_description'], dw_plant_images: ['plant_images_id', 'plant_symbol', 'plant_master_id', 'parent_master_id', 'plant_rank', 'plant_synonym_ind', 'plant_full_scientific_name', 'plant_full_scientific_name_without_author', 'plant_scientific_name_html', 'plant_sciname_sort', 'plant_family', 'plant_family_symbol', 'plant_primary_vernacular', 'plant_image_type', 'plant_image_purpose', 'provided_by', 'provided_by_sortname', 'scanned_by', 'scanned_by_sortname', 'originally_from', 'originally_from_sortname', 'author', 'author_sortname', 'contributorindividual', 'contrib_ind_sortname', 'contributororganization', 'contrib_org_sortname', 'plantauthorship', 'plant_author_sortname', 'artist', 'artist_sortname', 'copyrightholder', 'copyright_sortname', 'other', 'other_sortname', 'plant_reference_title', 'plant_reference_place', 'plant_reference_year', 'plant_publication_volume_nbr', 'plant_publication_issue', 'plant_reference_publication', 'plant_reference_media_type', 'plant_reference_source_type', 'plant_institution_name', 'plant_image_website_url', 'plant_source_email', 'plant_imagelocation', 'plant_imagecreationdate', 'plant_copyright_ind', 'plant_image_country_fullname', 'plant_image_country_abbr', 'plant_image_state', 'plant_image_state_abbr', 'plant_image_county', 'plant_image_city', 'plant_image_locality', 'plant_image_fips', 'plant_image_geoid', 'plant_image_notes', 'plant_image_primary_ind', 'plant_image_display_ind', 'plant_image_id', 'plant_country_identifier', 'plant_location_id', 'plant_country_subdivision_id', 'plant_reference_id', 'plant_image_last_updated', 'plant_location_last_updated', 'plant_image_cred_last_updated', 'plant_reference_last_updated', 'dw_record_updated'], d_lifespan: [], plant_duration: [], plant_global_conservation: [], plant_hybrid_formula: [], d_common_name_type: [], d_country: [], d_plant_image_purpose: ['plant_image_purpose_description'], d_plant_wetland: [], staging_plant_invasive: ['staging_plant_invasive_id', 'plant_symbol', 'plant_synonym', 'accepted_sciname', 'useifdifferent_sci', 'common_name', 'state_status', 'plant_master_id', 'plant_syn_id', 'common_name_id', 'state_status_id', 'location_abbr', 'location_code', 'location_name', 'creation_date', 'created_by', 'processed'], d_plant_wildlife_food: [], plant_protected_status: [], plant_reference: ['plant_reference_acronym', 'state_county_code', 'secondary_reference_title', 'reference_hyperlink'], d_plant_pollinator: [], plant_usage: ['plant_usage_id', 'plant_use_id', 'plant_location_characteristic_id', 'active_record_ind', 'creation_date', 'created_by', 'last_change_date', 'last_changed_by'], d_noxious_status: [], plant_notes: ['synonym_notes', 'subordinate_taxa_notes', 'legal_notes', 'noxious_notes', 'rarity_notes', 'wetland_notes', 'related_links_notes', 'wildlife_notes', 'sources_notes', 'characteristic_notes', 'pollinator_notes', 'cultural_notes', 'ethnobotany_notes'], d_protected_status_source: [], plant_data_source: ['plant_data_source_last_name', 'plant_data_source_first_name', 'plant_data_source_website_url'], d_plant_reference_purpose: [], d_noxious_status_source: [], audit_plant_master_tbl: ['plant_master_update_id', 'action_taken', 'plant_master_id', 'plant_hierarchy_id', 'plant_symbol', 'plant_status_id', 'plant_rank_id', 'plant_synonym_ind', 'plant_scientific_name', 'plant_author_name_id', 'plant_primary_vernacular_id', 'plant_revisor_id', 'full_scientific_name', 'full_scientific_name_html', 'full_scientific_name_without_author', 'is_active', 'parent_master_id', 'is_taxa', 'taxa_master_id', 'gsat', 'cover_crop', 'cultural_significant_ind', 'action_date', 'action_taken_by', 'action_generated_from'], audit_invasive_source: [], d_plant_wildlife_type: [], d_invasive_status_source: [], staging_plant_wetland: ['parent_region_id'], d_plant_wildlife_cover: [], d_plant_status: ['last_change_date', 'last_changed_by'], d_country_subdivision_type: [], entitlement: [], plant_ethnobotany: [], audit_plant_reference: ['plant_reference_acronym', 'plant_reference_second_title', 'plant_reference_place', 'state_county_code', 'plant_publication_volume_nbr', 'plant_website_url_text', 'plant_author', 'plant_publisher'], d_country_complete: ['end_date'], d_plant_occurrence_type: ['plant_occurrence_type_description'], plant_synonym_tbl: [], d_color: [], d_plant_use: ['plant_use_description', 'last_changed_by'], plant_wildlife: [], plant_data_sources: [], audit_plant_ref_association: ['plant_ref_assoc_id', 'plant_master_id', 'plant_literature_id', 'plant_reference_id', 'action_taken', 'action_date', 'is_active'], plant_image: ['plant_image_file_name', 'plant_image', 'plant_image_notes', 'plant_image_location_latitude', 'plant_image_location_longitude'], plant_unknown_tbl: [], plant_reference_source: [], plant_invasive_status: [], d_foliage_porosity: [], plant_region: [], plant_suitability_use: [], d_plant_nativity_region: [], plant_image_credit: [], plant_growth_requirements: [], d_plant_ethno_culture: ['plant_ethno_culture_notes'], plant_occurrence_location: [], plant_location_characteristic: ['plant_noxious_status_id'], audit_plant_location_common_name: ['plant_location_common_name_audit_id', 'action_taken', 'plant_master_id', 'plant_location_id', 'plant_primary_vernacular_id', 'action_taken_from', 'is_active', 'action_date', 'action_taken_by'], audit_plant_data_source: ['plant_data_source_email_address', 'contributor_id', 'plant_data_source_website_url'], linegeometries: ['id', 'shape', 'code'], d_foliage_texture: [], d_plant_nativity: [], plant_classifications_tbl: ['suborder', 'subfamily', 'classid_hybrid_author', 'taxquest'], d_plant_name_suffix: ['plant_name_suffix_description'], document_delete: ['Word Files', 'PDF files'], d_crop_type: [], audit_plant_synonym_tbl: ['plant_synonym_update_id', 'action_taken', 'plant_synonym_id', 'plant_master_id', 'synonym_plant_master_id', 'is_active', 'action_date', 'action_taken_by', 'action_generated_from'], plant_data_source_detail: ['plant_data_source_address', 'plant_data_source_city', 'plant_data_source_state', 'plant_data_source_phone', 'plant_data_source_affiliations'], d_extent: [], d_plant_taxonomic_status: ['last_changed_by'], d_shape_orientation: [], d_plant_ethno_use: ['plant_ethno_usage_definition'], '8ball_data': [], plant_reproduction: [], role_entitlement: [], plant_pollinator: [], d_plant_image_credit_type: [], staging_symbol_generator: ['reservedfor_id', 'formauthorid', 'varietyauthorid', 'subvarietyauthorid', 'subspeciesauthorid', 'speciesauthorid', 'genusauthorid', 'accepted_symbol', 'acceptedid'], d_plant_record_type: ['last_change_date', 'last_changed_by'], staging_plant_invasive_source: ['staging_plant_invasive_source_id', 'author', 'inv_year', 'hyperlink_txt', 'inv_url', 'location_abbr', 'location_code', 'location_name', 'creation_date', 'created_by', 'processed'], plant_master_image: ['plant_image_purpose_id'], plant_location_reference: ['plant_location_reference_id', 'plant_location_characteristic_id', 'plant_reference_id', 'plant_location_subject_id', 'plant_reference_purpose_id', 'creation_date', 'created_by', 'last_change_date', 'last_changed_by', 'active_record_ind'], alternative_crop: [], d_plant_action: [], d_rate: [], audit_plant_image: ['plant_image_audit_id', 'plant_master_id', 'plant_image_id', 'plant_reference_id', 'plant_image_type_id', 'plant_image_taken_date', 'plant_image_primary_ind', 'plant_image_display_ind', 'plant_image_copyrighted_ind', 'plant_image_stream_id', 'plant_image_location', 'plant_image_notes', 'action_taken', 'action_date', 'action_taken_by', 'active_record_ind'], plant_vascular: ['taxa_master_id'], d_plant_vernacular: [], state_gen_nonus_project_webmercator: ['objectid', 'shape', 'state_name', 'identifier'], audit_plant_invasive_status: ['plant_invasive_id'], d_plant_wetland_region: [], audit_plant_master_image: ['plant_master_image_audit_id', 'plant_master_id', 'plant_master_image_id', 'plant_image_purpose_id', 'action_taken', 'action_date', 'action_taken_by', 'active_record_ind'], audit_plant_work_basket: ['audit_work_basket_id', 'plant_work_basket_id', 'table_name', 'table_record_id', 'process_status_id', 'notes', 'action_date', 'action_taken_by'], plant_herbarium_image: ['plant_herbarium_image_id', 'plant_herbarium_id', 'plant_image_id', 'active_record_ind', 'creation_date', 'created_by', 'last_change_date', 'last_changed_by'], plant_cultural: [], d_plant_family_category: [], d_growth_form: [], audit_plant_wetland: ['plant_wetland_notes'], d_protected_status: [], audit_plant_image_credit: ['plant_image_credit_audit_id', 'plant_master_id', 'plant_image_credit_id', 'plant_image_id', 'plant_image_prefix_id', 'plant_image_credit_type_id', 'plant_image_data_source_id', 'plant_image_credit_display_ind', 'action_taken', 'action_date', 'action_taken_by', 'active_record_ind'], d_season: [], plants_work_basket: ['plant_work_basket_id', 'table_name', 'table_record_id', 'process_status_id', 'notes', 'creation_date', 'created_by', 'last_change_date', 'last_changed_by'], plant_location_common_name: [], plant_occurrence: ['plant_collection_nbr', 'plant_location_description', 'plant_specific_location_description', 'plant_habitat_description', 'plant_determination_date'], plant_data_reference: ['plant_publication_chapter'], audit_plant_image_library: ['plant_image_library_audit_id', 'plant_image_library_id', 'plant_image_id', 'plant_image_name', 'plant_image_new_name', 'plant_stream_id', 'action_taken', 'action_date', 'action_taken_by', 'active_record_ind'], d_plant_website_type: [], plant_common_name: ['last_change_date', 'last_changed_by'], d_commercial_availability: [], plant_spotlight: ['last_change_date', 'last_changed_by'], dw_plant_wetland: ['plant_dw_wetland_id', 'plant_wetland_symbol', 'plant_accepted_symbol', 'plant_master_id', 'parent_master_id', 'plant_rank', 'plant_synonym_ind', 'plant_scientific_name', 'plant_full_scientific_name', 'plant_full_scientific_name_without_author', 'plant_scientific_name_html', 'plant_sciname_sort', 'plant_family', 'plant_family_symbol', 'plant_primary_vernacular', 'plant_region', 'plant_subregion', 'plant_region_description', 'plant_region_abbreviation', 'plant_parent_region_abbreviation', 'plant_parent_region_description', 'plant_wetland_notes', 'plant_wetland_status_abbreviation', 'plant_wetland_status_description', 'plant_wetland_status_name', 'plant_hydrophyte_ind', 'plant_location_id', 'plant_location_characteristic_id', 'plant_wetland_status_id1', 'plant_wetland_region_id', 'plant_wetland_parent_id', 'plant_region_last_updated', 'plant_base_data_last_updated', 'plant_wetland_status_last_updated', 'plant_dw_record_last_updated'], plant_literature_location: [], d_plant_rank: ['display_sequence', 'last_change_date', 'last_changed_by'], alternative_crop_information: [], d_plant_image_type: [], plant_master_tbl: ['taxa_master_id'], role: [], d_plant_herbarium: ['plant_reference_id'], d_plant_duration: [], d_country_subdivision_category: [], d_country_subdivision: ['country_subdivision_level'], plant_location: ['state_county_code', 'plant_location_shape'], plant_ethnobotany_source: [], d_plant_reserved_status: [], staging_plant_wetland_import: ['plant_wetland_import_id', 'plant_scientific_name', 'plant_symbol', 'plant_synonym', 'wetland_symbol', 'hi', 'cb', 'ak', 'aw', 'agcp', 'emp', 'gp', 'mw', 'ncne', 'wmvc', 'aki', 'acp', 'cil', 'crb', 'iah', 'ial', 'iam', 'ngl', 'nbr', 'nsl', 'pda', 'sph', 'spi', 'ukk', 'wbrmnt', 'wgc', 'creation_date', 'created_by'], odmt_authorized: ['odmt_role1', 'odmt_role2', 'odmt_role3', 'last_changed_by'], d_plant_reference_type: [], plant_literature: [], d_plant_reserved_for: [], plants_document_remove: ['plants_doc_remove_id', 'plant_document_name', 'creation_date', 'created_by', 'last_change_date', 'last_changed_by', 'active_record_ind'], audit_plant_notes: ['synonym_notes', 'subordinate_taxa_notes', 'legal_notes', 'noxious_notes', 'rarity_notes', 'wetland_notes', 'related_links', 'wildlife_notes', 'sources_notes', 'characteristic_notes', 'pollinator_notes', 'cultural_notes', 'ethnobotany_notes'], d_plant_family: ['plant_family_alt_sym'], gsat_lkup: [], plant_related_website: ['plant_website_url_suffix'], d_conservation_status_rank: [], d_plant_growth_habit: [], d_state_county: ['coastal_county_ind', 'countyseat_geometry', 'state_county_geometry'], dw_plant_master_profile: ['plant_master_profile_id', 'plant_master_id', 'plant_symbol', 'plant_rank', 'plant_rank_id', 'plant_synonym_ind', 'plant_is_hybrid_ind', 'plant_full_scientific_name', 'plant_full_scientific_name_without_author', 'plant_scientific_name_html', 'plant_sciname_sort', 'plant_author', 'plant_author_id', 'plant_revisor', 'plant_revisor_id', 'plant_primary_vernacular', 'plant_primary_vernacular_id', 'plant_state_vernacular', 'plant_vernacular_state', 'plant_vernacular_trademark', 'plant_other_common_names', 'plant_group', 'plant_category', 'plant_family', 'plant_family_symbol', 'plant_family_vernacular', 'plant_noxious_ind', 'plant_global_rarity_ind', 'plant_us_rarity_ind', 'plant_wetland_ind', 'plant_invasive_ind', 'plant_vascular_ind', 'plant_duration1', 'plant_duration2', 'plant_duration3', 'plant_growth1', 'plant_growth2', 'plant_growth3', 'plant_growth4', 'plant_nat_l48', 'plant_nat_ak', 'plant_nat_hi', 'plant_nat_pr', 'plant_nat_vi', 'plant_nat_nav', 'plant_nat_can', 'plant_nat_gl', 'plant_nat_spm', 'plant_nat_na', 'plant_nat_pb', 'plant_nat_pfa', 'plantguide_pdf', 'plantguide_docx', 'factsheet_pdf', 'factsheet_docx', 'plant_master_notes', 'plant_synonym_notes', 'plant_subordinate_taxa_notes', 'plant_legal_notes', 'plant_taxonomic_status_suffix', 'gsat', 'cover_crop', 'cultural_significant_ind', 'is_taxa', 'taxa_master_id', 'parent_master_id', 'plant_hierarchy_id', 'plant_parent_hierarchy_id', 'plant_taxa_hierarchy_id', 'plant_hybrid_parent1', 'plant_hybrid_parent2', 'plant_hierarchy_level', 'plant_kingdom', 'plant_subkingdom', 'plant_superdivision', 'plant_division', 'plant_subdivision', 'plant_class', 'plant_order', 'plant_suborder', 'plant_subfamily', 'plant_xgenus', 'plant_genus', 'plant_xspecies', 'plant_species', 'plant_ssp', 'plant_xsubsp', 'plant_subspecies', 'plant_var', 'plant_xvariety', 'plant_variety', 'plant_subvariety', 'plant_f', 'plant_forma', 'bauthor', 'tauthor', 'qauthor', 'nomenclature', 'unaccept_reason', 'plant_base_data_last_updated', 'plant_classification_data_last_updated', 'dw_record_updated'], plant_growth_habit: [], plant_document_audit: [], state_nrcs_download: [], plant_master_document: [], d_toxicity: [], d_plant_data_source_type: ['plant_data_source_type_description'], d_plant_noxious_status: ['plant_noxious_status_name'], d_conservation_status_qualifier: [], d_shade_tolerance: [], state_gen_us_project_webmercator: ['objectid', 'shape', 'state_name', 'identifier'], d_process_status: ['process_status_id', 'process_status_name', 'process_status_definition', 'creation_date', 'created_by', 'last_change_date', 'last_changed_by', 'active_record_ind'], audit_plant_ref_source: [], audit_noxious_source: ['source_audit_id', 'action_taken', 'noxious_status_source_id', 'plant_location_id', 'noxious_status_sourc_text', 'is_active', 'action_date', 'action_taken_by'], plant_family_category: [], d_plant_image_prefix: [], plant_name_suffix: [], d_invasive_status: [], plant_morphology_physiology: ['hmaba_id', 'hmaba_display', 'ham_id', 'ham_display'],
    };

    send(res, empty);
  }
  let tables = req.query.table ? [req.query.table] : [];

  if (!tables.length) {
    const results = await pool.query(`
      SELECT DISTINCT table_name
      FROM information_schema.columns
      WHERE table_schema = 'plants3';
    `);

    tables = results.rows.map((row) => row.table_name);
  }

  const empty = {};

  // eslint-disable-next-line no-restricted-syntax
  for await (const table of tables) {
    empty[table] = [];
    const results = await pool.query(`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_name = '${table}';
    `);

    const columns = results.rows.map((row) => row.column_name);
    // eslint-disable-next-line no-restricted-syntax
    for await (const col of columns) {
      const result = await pool.query(`
        SELECT 1
        FROM plants3."${table}" as p
        WHERE p."${col}" IS NOT NULL
        limit 1
      `);

      if (!result.rows.length) {
        empty[table].push(col);
      }
    }
  }

  send(res, empty);
}; // routePlantsEmptyColumns

const routeVegspecCharacteristics = async (req = testRequest, res = testResponse) => {
  const createCharacteristics = async () => {
    const sq = `
      CREATE TABLE IF NOT EXISTS plants3.plant_morphology_physiology_backup AS SELECT * FROM plants3.plant_morphology_physiology;
      CREATE TABLE IF NOT EXISTS plants3.plant_growth_requirements_backup AS SELECT * FROM plants3.plant_growth_requirements;
      CREATE TABLE IF NOT EXISTS plants3.plant_reproduction_backup AS SELECT * FROM plants3.plant_reproduction;
      CREATE TABLE IF NOT EXISTS plants3.plant_suitability_use_backup AS SELECT * FROM plants3.plant_suitability_use;
  
      UPDATE plants3.plant_morphology_physiology a
      SET plant_master_id = b.plant_master_id
      FROM plants3.plant_synonym_tbl b
      WHERE a.plant_master_id = b.synonym_plant_master_id;

      UPDATE plants3.plant_growth_requirements a
      SET plant_master_id = b.plant_master_id
      FROM plants3.plant_synonym_tbl b
      WHERE a.plant_master_id = b.synonym_plant_master_id;

      UPDATE plants3.plant_reproduction a
      SET plant_master_id = b.plant_master_id
      FROM plants3.plant_synonym_tbl b
      WHERE a.plant_master_id = b.synonym_plant_master_id;

      UPDATE plants3.plant_suitability_use a
      SET plant_master_id = b.plant_master_id
      FROM plants3.plant_synonym_tbl b
      WHERE a.plant_master_id = b.synonym_plant_master_id;
      
      DROP TABLE IF EXISTS plants3.synonyms;
      SELECT DISTINCT a.*, b.plant_symbol AS psymbol, c.plant_symbol AS ssymbol
      INTO plants3.synonyms
      FROM (
        SELECT a.plant_master_id AS pid, b.synonym_plant_master_id
        FROM plants3.plant_synonym_tbl a
        JOIN plants3.plant_synonym_tbl b
        USING (plant_master_id)
      ) a
      JOIN plants3.plant_master_tbl b
      ON a.pid = b.plant_master_id
      JOIN plants3.plant_master_tbl c
      ON synonym_plant_master_id = c.plant_master_id;
      CREATE INDEX ON plants3.synonyms (pid);
      CREATE INDEX ON plants3.synonyms (synonym_plant_master_id);
            
      DROP TABLE IF EXISTS plants3.characteristics;
      SELECT * INTO plants3.characteristics
      FROM (
        SELECT DISTINCT * FROM (
          SELECT
            p.plant_symbol,
            plant_master_id,
            coalesce(m.cultivar_name, g.cultivar_name, r.cultivar_name, s.cultivar_name) AS cultivar,
            full_scientific_name_without_author,
            primary_vernacular,
            plant_duration_name,
            plant_nativity_type,
            plant_nativity_region_name,
            plant_growth_habit_name,
            cover_crop,
            active_growth_period.season_name AS active_growth_period,
            after_harvest_regrowth_rate.rate_name AS after_harvest_regrowth_rate,
            bloat_potential.extent_name AS bloat_potential,
            c_n_ratio.extent_name AS c_n_ratio,
            coppice_potential_ind,
            fall_conspicuous_ind,
            fire_resistant_ind,
            color_name,
            flower_conspicuous_ind,
            summer.foliage_porosity_name AS summer,
            winter.foliage_porosity_name AS winter,
            foliage_texture_name,
            fruit_seed_conspicuous_ind,
            growth_form_name,
            growth_rate.rate_name AS growth_rate,
            height_max_at_base_age,
            height_at_maturity,
            known_allelopath_ind,
            leaf_retention_ind,
            lifespan_name,
            low_growing_grass_ind,
            nitrogen_fixation_potential.extent_name AS nitrogen_fixation_potential,
            resprout_ability_ind,
            shape_orientation_name,
            toxicity_name,
            
            pd_max_range,
            ff_min_range,
            ph_min_range,
            ph_max_range,
            density_min_range,
            precip_min_range,
            precip_max_range,
            root_min_range,
            temp_min_range,
            commercial_availability_id,
            fruit_seed_abundance_id,
            propagated_by_bare_root_ind,
            propagated_by_bulb_ind,
        
            coarse_texture_soil_adaptable_ind,
            medium_texture_soil_adaptable_ind,
            fine_texture_soil_adaptable_ind,
            anaerobic_tolerance.extent_name AS anaerobic_tolerance,
            caco3_tolerance.extent_name AS caco3_tolerance,
            cold_stratification_required_ind,
            drought_tolerance.extent_name AS drought_tolerance,
            fire_tolerance.extent_name AS fire_tolerance,
            hedge_tolerance.extent_name AS hedge_tolerance,
            moisture_usage.extent_name AS moisture_usage,
            soil_ph_tolerance_min,
            soil_ph_tolerance_max,
            precipitation_tolerance_min, 
            precipitation_tolerance_max,
            salinity_tolerance.extent_name AS salinity_tolerance,
            shade_tolerance_name,
            temperature_tolerance_min,
        
            bloom_period.season_name AS bloom_period,
            fruit_seed_period_start.season_name AS fruit_seed_period_start,
            fruit_seed_period_end.season_name AS fruit_seed_period_end,
            fruit_seed_persistence_ind,
            seed_per_pound,
            seed_spread_rate.rate_name AS seed_spread_rate,
            seedling_vigor.extent_name AS seedling_vigor,
            vegetative_spread_rate.rate_name AS vegetative_spread_rate,
        
            berry_nut_seed_product_ind,
            fodder_product_ind,
            palatability_browse.extent_name AS palatability_browse,
            palatability_graze.extent_name AS palatability_graze,
            palatability_human_ind,
            protein_potential.extent_name AS protein_potential,

            frost_free_days_min,
            planting_density_min,
            root_depth_min

          FROM plants3.plant_master_tbl p
          
          LEFT JOIN plants3.plant_classifications_tbl USING (plant_master_id)
          LEFT JOIN (
            SELECT
              STRING_AGG(plant_duration_name, ', ' ORDER BY plant_duration_name) AS plant_duration_name,
              plant_master_id
            FROM plants3.plant_duration
            LEFT JOIN plants3.d_plant_duration USING (plant_duration_id)
            GROUP BY plant_master_id
          ) pd USING (plant_master_id)
          LEFT JOIN (
            SELECT
              STRING_AGG(plant_growth_habit_name, ', ' ORDER BY plant_growth_habit_name) AS plant_growth_habit_name,
              plant_master_id
            FROM plants3.plant_growth_habit 
            LEFT JOIN plants3.d_plant_growth_habit USING (plant_growth_habit_id)
            GROUP BY plant_master_id
          ) pgh USING (plant_master_id)
          LEFT JOIN plants3.plant_morphology_physiology m USING (plant_master_id)
          LEFT JOIN plants3.plant_growth_requirements g USING (plant_master_id, cultivar_name)
          LEFT JOIN plants3.plant_reproduction r USING (plant_master_id, cultivar_name)
          LEFT JOIN plants3.plant_suitability_use s USING (plant_master_id, cultivar_name)

          LEFT JOIN (
            SELECT
              plant_nativity_type,
              STRING_AGG(nativity.plant_nativity_region_name, ', ' ORDER BY nativity.plant_nativity_region_name) AS plant_nativity_region_name,
              plant_master_id
            FROM plants3.nativity
            LEFT JOIN plants3.d_plant_nativity_region USING (plant_nativity_region_id)
            GROUP BY plant_master_id, plant_nativity_type
          ) nat USING (plant_master_id)

          LEFT JOIN plants3.d_season active_growth_period ON m.active_growth_period_id=active_growth_period.season_id
          LEFT JOIN plants3.d_rate after_harvest_regrowth_rate ON m.after_harvest_regrowth_rate_id=after_harvest_regrowth_rate.rate_id
          LEFT JOIN plants3.d_extent bloat_potential ON m.bloat_potential_id=bloat_potential.extent_id
          LEFT JOIN plants3.d_extent c_n_ratio ON m.c_n_ratio_id=c_n_ratio.extent_id
          LEFT JOIN plants3.d_color c ON m.flower_color_id = c.color_id
          LEFT JOIN plants3.d_foliage_porosity summer ON m.summer_foliage_porosity_id=summer.foliage_porosity_id
          LEFT JOIN plants3.d_foliage_porosity winter ON m.winter_foliage_porosity_id=winter.foliage_porosity_id
          LEFT JOIN plants3.d_foliage_texture foliage_texture_name ON m.foliage_texture_id=foliage_texture_name.foliage_texture_id
          LEFT JOIN plants3.d_growth_form gf ON m.growth_form_id=gf.growth_form_id
          LEFT JOIN plants3.d_rate growth_rate ON m.growth_rate_id=growth_rate.rate_id
          LEFT JOIN plants3.d_lifespan USING (lifespan_id)
          LEFT JOIN plants3.d_extent nitrogen_fixation_potential ON m.nitrogen_fixation_potential_id=nitrogen_fixation_potential.extent_id
          LEFT JOIN plants3.d_shape_orientation q ON m.shape_orientation_id=q.shape_orientation_id
          LEFT JOIN plants3.d_toxicity USING (toxicity_id)
          LEFT JOIN plants3.d_extent anaerobic_tolerance ON g.anaerobic_tolerance_id=anaerobic_tolerance.extent_id
          LEFT JOIN plants3.d_extent caco3_tolerance ON g.caco3_tolerance_id=caco3_tolerance.extent_id
          LEFT JOIN plants3.d_extent drought_tolerance ON g.drought_tolerance_id=drought_tolerance.extent_id
          LEFT JOIN plants3.d_extent fire_tolerance ON g.fire_tolerance_id=fire_tolerance.extent_id
          LEFT JOIN plants3.d_extent hedge_tolerance ON g.hedge_tolerance_id=hedge_tolerance.extent_id
          LEFT JOIN plants3.d_extent moisture_usage ON g.moisture_usage_id = moisture_usage.extent_id
          LEFT JOIN plants3.d_extent salinity_tolerance ON g.salinity_tolerance_id = salinity_tolerance.extent_id
          LEFT JOIN plants3.d_shade_tolerance USING (shade_tolerance_id)
        
          LEFT JOIN plants3.d_season bloom_period ON r.bloom_period_id=bloom_period.season_id
          LEFT JOIN plants3.d_season fruit_seed_period_start ON r.fruit_seed_period_start_id=fruit_seed_period_start.season_id
          LEFT JOIN plants3.d_season fruit_seed_period_end ON r.fruit_seed_period_end_id=fruit_seed_period_end.season_id
          LEFT JOIN plants3.d_rate seed_spread_rate ON r.seed_spread_rate_id=seed_spread_rate.rate_id
          LEFT JOIN plants3.d_extent seedling_vigor ON r.seedling_vigor_id = seedling_vigor.extent_id
          LEFT JOIN plants3.d_rate vegetative_spread_rate ON r.vegetative_spread_rate_id=vegetative_spread_rate.rate_id
        
          LEFT JOIN plants3.d_extent palatability_browse ON s.palatability_browse_id = palatability_browse.extent_id
          LEFT JOIN plants3.d_extent palatability_graze ON s.palatability_graze_id = palatability_graze.extent_id
          LEFT JOIN plants3.d_extent protein_potential ON s.protein_potential_id = protein_potential.extent_id
          WHERE coalesce(
            active_growth_period::text, after_harvest_regrowth_rate::text, bloat_potential::text, c_n_ratio::text, coppice_potential_ind::text,
            fall_conspicuous_ind::text, fire_resistant_ind::text, color_name::text, flower_conspicuous_ind::text, summer::text, winter::text,
            fruit_seed_conspicuous_ind::text, growth_form_name::text, growth_rate::text, height_max_at_base_age::text, height_at_maturity::text,
            known_allelopath_ind::text, leaf_retention_ind::text, lifespan_name::text, low_growing_grass_ind::text, nitrogen_fixation_potential::text,
            resprout_ability_ind::text, shape_orientation_name::text, toxicity_name::text, coarse_texture_soil_adaptable_ind::text,
            medium_texture_soil_adaptable_ind::text, fine_texture_soil_adaptable_ind::text, anaerobic_tolerance::text, caco3_tolerance::text,
            cold_stratification_required_ind::text, drought_tolerance::text, fire_tolerance::text, hedge_tolerance::text, moisture_usage::text,
            soil_ph_tolerance_min::text, soil_ph_tolerance_max::text, precipitation_tolerance_min::text, precipitation_tolerance_max::text,
            salinity_tolerance::text, shade_tolerance_name::text, temperature_tolerance_min::text, bloom_period::text, fruit_seed_period_start::text,
            fruit_seed_period_end::text, fruit_seed_persistence_ind::text, seed_per_pound::text, seed_spread_rate::text, seedling_vigor::text,
            vegetative_spread_rate::text, berry_nut_seed_product_ind::text, fodder_product_ind::text,
            palatability_browse::text, palatability_graze::text,
            palatability_human_ind::text, protein_potential::text,
            plant_nativity_region_name
          ) > ''
          OR p.plant_symbol in (SELECT plant_symbol FROM plants3.states)
        ) alias
        ORDER BY 1, 2, 3
      ) alias;

      INSERT INTO plants3.characteristics (plant_symbol, cultivar, plant_nativity_region_name, plant_nativity_type) (
        SELECT DISTINCT
          a.plant_symbol, a.cultivar_name,
          CASE WHEN state IN ('HI', 'AK') THEN state ELSE 'Lower 48 States' END,
          'Native'
        FROM plants3.states a
        LEFT JOIN plants3.characteristics b
        ON a.plant_symbol = b.plant_symbol AND COALESCE(a.cultivar_name, '') = COALESCE(b.cultivar, '')
        WHERE b.plant_symbol IS NULL
      );

      UPDATE plants3.characteristics a
      SET  (plant_master_id,   full_scientific_name_without_author,   primary_vernacular,   plant_duration_name,   plant_growth_habit_name,
            cover_crop) =
        ROW(b.plant_master_id, b.full_scientific_name_without_author, b.primary_vernacular, b.plant_duration_name, b.plant_growth_habit_name,
            b.cover_crop)
      FROM plants3.characteristics b
      WHERE
        a.plant_symbol = b.plant_symbol
        AND a.full_scientific_name_without_author IS NULL AND b.full_scientific_name_without_author IS NOT NULL;
    
      CREATE INDEX ON plants3.characteristics (plant_symbol);
      CREATE INDEX ON plants3.characteristics (plant_master_id);
    `;

    await pool.query({ text: sq, multi: true });
  }; // createCharacteristics

  if (req.query.create) {
    await createCharacteristics();
  } else {
    try {
      let results = await pool.query('SELECT COUNT(*) FROM plants3.characteristics');
      if (+results.rows[0].count === 0) {
        await createCharacteristics();
      }

      results = await pool.query('SELECT COUNT(*) FROM plants3.synonyms');
      if (+results.rows[0].count === 0) {
        await createCharacteristics();
      }
    } catch (error) {
      await createCharacteristics();
    }
  }

  let symbols = [];

  const mlra = req.query.mlra || '';
  const state = req.query.state?.toUpperCase() || '';
  const allowedCultivars = {};
  let stateData = [];
  if (state) {
    stateData = (
      await pool.query(`
        SELECT
          plant_master_id,
          b.plant_symbol,
          parameter,
          value,
          cultivar_name,
          sci_name,
          primary_vernacular,
          plant_duration_name,
          plant_nativity_type,
          plant_nativity_region_name,
          plant_growth_habit_name,
          cover_crop
        FROM plants3.states a
        LEFT JOIN plants3.plant_classifications_tbl b USING (plant_symbol)
        LEFT JOIN plants3.plant_master_tbl USING (plant_master_id)
        LEFT JOIN plants3.plant_duration USING (plant_master_id)
        LEFT JOIN plants3.d_plant_duration USING (plant_duration_id)
        LEFT JOIN plants3.nativity USING (plant_master_id)
        LEFT JOIN plants3.plant_growth_habit USING (plant_master_id)
        LEFT JOIN plants3.d_plant_growth_habit USING (plant_growth_habit_id)
        WHERE state = $1
        ORDER BY a.plant_symbol, parameter
      `, [state])
    ).rows;
    if (stateData.length) {
      if (mlra) {
        const stateSymbols = stateData
          .filter((row) => row.parameter === 'mlra' && row.value.split(',').includes(mlra))
          .map((row) => row.plant_symbol);

        stateData = stateData.filter((row) => stateSymbols.includes(row.plant_symbol));
      }

      stateData.forEach((row) => {
        if (!symbols.includes(row.plant_symbol)) {
          symbols.push(row.plant_symbol);
        }
        if (row.cultivar_name) {
          allowedCultivars[row.plant_symbol] = allowedCultivars[row.plant_symbol] || [];
          allowedCultivars[row.plant_symbol].push(row.cultivar_name);
        }
      });
    }
  }

  // res.send({ symbols, synonyms }); return;

  if (mlra && !symbols.length) {
    // from Access database
    symbols = await pool.query(`
      SELECT DISTINCT plant_symbol
      FROM mlra_species
      WHERE mlra='${mlra}'
    `);
    symbols = symbols.rows.map((row) => row.plant_symbol);
  } else if (req.query.symbols) {
    symbols = req.query.symbols.split(',');
  }

  const querySymbols = symbols.map((symbol) => `'${symbol}'`);

  let stateCond = '';
  let regionRegex = 'plant_nativity_region_name';
  let groupBy = '';
  if (state === 'AK') {
    stateCond = ` AND plant_nativity_region_name ~ 'Alaska'`;
    regionRegex = `REGEXP_REPLACE(plant_nativity_region_name, '.*Alaska.*', 'Alaska')`;
  } else if (state === 'HI') {
    stateCond = ` AND plant_nativity_region_name ~ 'Hawaii'`;
    regionRegex = `REGEXP_REPLACE(plant_nativity_region_name, '.*Hawaii.*', 'Hawaii')`;
  } else if (state) {
    stateCond = ` AND plant_nativity_region_name ~ 'Lower 48'`;
    regionRegex = `REGEXP_REPLACE(plant_nativity_region_name, '.*Lower 48 States.*', 'Lower 48 States')`;
  }

  const columns = `
    plant_symbol,plant_master_id,cultivar,full_scientific_name_without_author,primary_vernacular,plant_duration_name,
    ${state ? `STRING_AGG(plant_nativity_type, ', ' ORDER BY plant_nativity_type) AS plant_nativity_type` : 'plant_nativity_type'},
    ${regionRegex} AS plant_nativity_region_name,
    plant_growth_habit_name,cover_crop,active_growth_period,after_harvest_regrowth_rate,bloat_potential,c_n_ratio,
    coppice_potential_ind,fall_conspicuous_ind,fire_resistant_ind,color_name,flower_conspicuous_ind,summer,winter,foliage_texture_name,
    fruit_seed_conspicuous_ind,growth_form_name,growth_rate,height_max_at_base_age,height_at_maturity,known_allelopath_ind,leaf_retention_ind,
    lifespan_name,low_growing_grass_ind,nitrogen_fixation_potential,resprout_ability_ind,shape_orientation_name,toxicity_name,pd_max_range,
    ff_min_range,ph_min_range,ph_max_range,density_min_range,precip_min_range,precip_max_range,root_min_range,temp_min_range,
    commercial_availability_id,fruit_seed_abundance_id,propagated_by_bare_root_ind,propagated_by_bulb_ind,coarse_texture_soil_adaptable_ind,
    medium_texture_soil_adaptable_ind,fine_texture_soil_adaptable_ind,anaerobic_tolerance,caco3_tolerance,cold_stratification_required_ind,
    drought_tolerance,fire_tolerance,hedge_tolerance,moisture_usage,soil_ph_tolerance_min,soil_ph_tolerance_max,precipitation_tolerance_min,
    precipitation_tolerance_max,salinity_tolerance,shade_tolerance_name,temperature_tolerance_min,bloom_period,fruit_seed_period_start,
    fruit_seed_period_end,fruit_seed_persistence_ind,seed_per_pound,seed_spread_rate,seedling_vigor,vegetative_spread_rate,
    berry_nut_seed_product_ind,fodder_product_ind,palatability_browse,palatability_graze,palatability_human_ind,protein_potential,
    frost_free_days_min,planting_density_min,root_depth_min
  `;

  if (state) {
    groupBy = `
      GROUP BY
      plant_symbol,plant_master_id,cultivar,full_scientific_name_without_author,primary_vernacular,plant_duration_name,
      ${regionRegex},
      plant_growth_habit_name,cover_crop,active_growth_period,after_harvest_regrowth_rate,bloat_potential,c_n_ratio,
      coppice_potential_ind,fall_conspicuous_ind,fire_resistant_ind,color_name,flower_conspicuous_ind,summer,winter,foliage_texture_name,
      fruit_seed_conspicuous_ind,growth_form_name,growth_rate,height_max_at_base_age,height_at_maturity,known_allelopath_ind,leaf_retention_ind,
      lifespan_name,low_growing_grass_ind,nitrogen_fixation_potential,resprout_ability_ind,shape_orientation_name,toxicity_name,pd_max_range,
      ff_min_range,ph_min_range,ph_max_range,density_min_range,precip_min_range,precip_max_range,root_min_range,temp_min_range,
      commercial_availability_id,fruit_seed_abundance_id,propagated_by_bare_root_ind,propagated_by_bulb_ind,coarse_texture_soil_adaptable_ind,
      medium_texture_soil_adaptable_ind,fine_texture_soil_adaptable_ind,anaerobic_tolerance,caco3_tolerance,cold_stratification_required_ind,
      drought_tolerance,fire_tolerance,hedge_tolerance,moisture_usage,soil_ph_tolerance_min,soil_ph_tolerance_max,precipitation_tolerance_min,
      precipitation_tolerance_max,salinity_tolerance,shade_tolerance_name,temperature_tolerance_min,bloom_period,fruit_seed_period_start,
      fruit_seed_period_end,fruit_seed_persistence_ind,seed_per_pound,seed_spread_rate,seedling_vigor,vegetative_spread_rate,
      berry_nut_seed_product_ind,fodder_product_ind,palatability_browse,palatability_graze,palatability_human_ind,protein_potential,
      frost_free_days_min,planting_density_min,root_depth_min
    `;
  }

  // console.log(groupBy);

  const sq = querySymbols.length
    ? `
        SELECT ${columns} FROM plants3.characteristics
        WHERE plant_symbol IN (${querySymbols}) ${stateCond}
        ${groupBy}
      `
    : `
        SELECT ${columns} FROM plants3.characteristics
        WHERE active_growth_period IS NOT NULL ${stateCond}
        ${groupBy}
      `;

  // console.log(sq);
  console.time('query');
  const results = (await pool.query(sq)).rows;
  console.timeEnd('query'); // 1s

  console.time('filter');
  let finalResults = results
    .sort((a, b) => a.plant_symbol.localeCompare(b.plant_symbol) || (a.cultivar || '').localeCompare(b.cultivar || ''))
    .filter((a, i, arr) => JSON.stringify(a) !== JSON.stringify(arr[i - 1]));

  if (symbols.length) {
    finalResults = finalResults.filter((a) => symbols.includes(a.plant_symbol));
  }

  if (Object.keys(allowedCultivars).length) {
    finalResults = finalResults.filter((row) => !row.cultivar || allowedCultivars[row.plant_symbol]?.includes(row.cultivar));
  }

  stateData.forEach((row) => {
    if (
      symbols.includes(row.plant_symbol)
      && !/mlra|cps/.test(row.parameter)
    ) {
      let obj = finalResults.find((frow) => (
        (frow.plant_symbol === row.plant_symbol)
        && ((frow.cultivar || '') === (row.cultivar_name || ''))
      ));

      if (!obj) {
        obj = { ...finalResults[0] };
        Object.keys(obj).forEach((key) => { obj[key] = null; });
        finalResults.push(obj);
      }
      obj.plant_symbol = row.plant_symbol;
      obj.cultivar = row.cultivar_name;
      obj[row.parameter] = row.value;
      obj.full_scientific_name_without_author = row.sci_name;
      obj.plant_master_id = row.plant_master_id;
      obj.primary_vernacular = row.primary_vernacular;

      const add = (parm) => {
        if (
          (state === 'AK' && row.plant_nativity_region_name === 'Alaska')
          || (state === 'HI' && row.plant_nativity_region_name === 'Hawaii')
          || (row.plant_nativity_region_name === 'Lower 48 States')
        ) {
          obj.plant_nativity_region_name = row.plant_nativity_region_name;
          if (row[parm] && !obj[parm]?.includes(row[parm])) {
            if (!obj[parm]) {
              obj[parm] = row[parm];
            } else {
              obj[parm] = ((obj[parm] || '').split(', ')) || [];
              obj[parm].push(row[parm]);
              obj[parm] = obj[parm].sort().join(', ');
            }
          }
        }
      }; // add

      if (row.plant_nativity_region_name === 'Lower 48 States') {
        obj.plant_nativity_region_name = row.plant_nativity_region_name;
        add('plant_duration_name');
        add('plant_nativity_type');
        add('plant_growth_habit_name');
        obj.cover_crop = row.cover_crop;
      }
    }
  });

  console.timeEnd('filter'); // 300ms

  finalResults = finalResults.sort((a, b) => a.plant_symbol.localeCompare(b.plant_symbol));

  if (!finalResults.length) {
    res.send([]);
    return;
  }

  send(res, finalResults);
}; // routeVegspecCharacteristics

const routeVegspecDeleteState = (req, res) => {
  simpleQuery(
    'DELETE FROM plants3.states where state=$1',
    [req.query.state],
    res,
  );
}; // routeVegspecDeleteState

const routeVegspecSaveState = async (req, res) => {
  const {
    state, symbol, cultivar, parameter, value, note,
  } = req.query;

  console.log({
    state, symbol, cultivar, parameter, value, note,
  });
  // console.log(state, symbol, cultivar, parameter, value, notes);

  const symbols = symbol.split(',');
  const cultivars = (cultivar || '').split(',');
  const parameters = parameter.split(',');
  const values = value.split(';');
  const notes = (note || '').split(';');

  pool.query('DROP TABLE IF EXISTS plants3.characteristics');

  try {
    let i = 0;
    // eslint-disable-next-line no-restricted-syntax
    for (const sym of symbols) {
      // eslint-disable-next-line no-await-in-loop
      await pool.query(
        'INSERT INTO plants3.states (state, plant_symbol, cultivar_name, parameter, value, notes) VALUES ($1, $2, $3, $4, $5, $6)',
        [state, sym || null, cultivars[i] || null, parameters[i] || null, values[i] || null, notes[i] || null],
      );
      i += 1;
    }
    send(res, { status: 'Success' });
  } catch (error) {
    console.error(error);
    send(res, { error });
  }
}; // routeVegspecSaveState

const routeVegspecState = async (req = testRequest, res = testResponse) => {
  simpleQuery(
    'select * from plants3.states where state=$1',
    [req.query.state],
    res,
  );
}; // routeVegspecState

const routePlantsTable = (req = testRequest, res = testResponse) => {
  const table = safeQuery(req, 'table');
  const sq = `select * from plants3.${table}`;

  simpleQuery(sq, [], res, true);
}; // routePlantsTable

const routeFrost = (req = testRequest, res = testResponse) => {
  const query = () => {
    const lat = lats ? lats[0] : req.query.lat;
    const lon = lons ? lons[0] : req.query.lon;

    const sq = `
      select * from frost.frost
      where 
        firstfreeze is not null and
        firstfrost is not null and
        lastfreeze is not null and
        lastfrost is not null and
        sqrt(power(lat - ${lat}, 2) + power(lon - ${lon}, 2)) < 0.7
      order by sqrt(power(lat - ${lat}, 2) + power(lon - ${lon}, 2))
      limit 1
    `;

    pool.query(
      sq,
      (err, results) => {
        if (err) {
          debug(err, res, 500);
        } else if (results.rows.length) {
          send(res, results.rows[0]);
        } else {
          send(res, {});
        }
      },
    );
  }; // query

  if (location) {
    getLocation(req, query);
  } else {
    query();
  }
}; // routeFrost

/**
 * Route handler for fetching yearly temperature and precipitation data.
 *
 * @param {Object} req - The request object.
 * @param {Object} res - The response object.
 * @returns {void}
 *
 * The tables were created as below.
 * Run at the beginning of each year, changing the year to the previous year (currently 2022).
 * Also change y2 to the previous year.
 *
 * DO $$
 * DECLARE
 *     tb TEXT;
 *     yearly_table_name TEXT;
 * BEGIN
 *     FOR tb IN
 *         SELECT table_name
 *         FROM information_schema.tables
 *         WHERE table_name LIKE 'nldas_hourly_%2022' AND table_schema = 'weather'
 *         ORDER BY table_name
 *     LOOP
 *         yearly_table_name := 'yearly_' || substring(tb, 14);
 *
 *         EXECUTE 'DROP TABLE IF EXISTS weather.' || yearly_table_name;
 *
 *         EXECUTE '
 *             CREATE TABLE weather.' || yearly_table_name || ' AS
 *             SELECT
 *               lat, lon,
 *               min(a.air_temperature) AS min_air_temperature,
 *               max(a.air_temperature) AS max_air_temperature,
 *               sum(a.precipitation) as sum_precipitation
 *             FROM weather.' || tb || ' AS a
 *             GROUP BY lat, lon';
 *
 *         RAISE NOTICE 'Created table: weather.%', yearly_table_name;
 *         PERFORM pg_sleep(1);
 *     END LOOP;
 * END $$;
*/
const routeYearly = (req = testRequest, res = testResponse) => {
  const query = () => {
    /**
     * Destructuring request query parameters.
     * @type {number} year - The year.
     * @type {number} year1 - The starting year (default: year).
     * @type {number} year2 - The ending year (default: year or year1).
     * @type {number} lat - The latitude.
     * @type {number} lon - The longitude.
     */

    const y1 = 2018;
    const y2 = 2022;

    const year = req.query.year || `${y1}-${y2}`;

    let [year1, year2] = year.toString().split('-');

    year1 = clamp(year1, y1, y2);
    year2 = clamp(year2 || year1, y1, y2);

    const lat = lats?.[0] || req.query.lat;
    const lon = lons?.[0] || req.query.lon;

    // SQL query for fetching yearly temperature data.
    const sq = `
      SELECT
        ${year1}${year2 !== year1 ? ` || '-' || ${year2}` : ''} as year,
        ${lat} as lat, ${lon} as lon,
        min(min_air_temperature) AS min_air_temperature,
        max(max_air_temperature) AS max_air_temperature,
        avg(sum_precipitation) AS avg_precipitation
      FROM (
        ${range(year1, year2).map((y) => `
            SELECT * FROM
            weather.yearly_${Math.trunc(NLDASlat(lat))}_${Math.trunc(-NLDASlon(lon))}_${y}
            WHERE lat=${NLDASlat(lat)} and lon=${NLDASlon(lon)}
          `).join(`
            UNION ALL
          `)}
      ) a
      GROUP BY lat, lon;
    `;

    console.log(sq);

    // Executing the SQL query.
    pool.query(
      sq,
      (err, results) => {
        if (err) {
          debug(err, res, 500);
        } else {
          send(res, results.rows);
        }
      },
    );
  }; // query

  if (location) {
    getLocation(req, query);
  } else {
    query();
  }
}; // routeYearly

async function routeTest(req, res) {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Transfer-Encoding', 'chunked');
  res.setHeader('x-content-type-options', 'nosniff');
  res.flushHeaders();

  async function testGoogleMapsAPI() {
    const response = await axios.get('https://api64.ipify.org/?format=json');
    const ip2 = response.data.ip;

    const { data } = await axios.get(
      `https://maps.googleapis.com/maps/api/timezone/json?location=35.43,-95&timestamp=0&key=${googleAPIKey}`,
    );

    let results = `IP: ${myip.address()}  or  ${ip2}\nGoogle Maps API: ${JSON.stringify(data)}\n${googleAPIKey}`;

    if (data.status !== 'OK') {
      results = `FAILED: ${results}`;
    }
    console.log(results);
    send(res, results);
  } // GoogleMapsAPI

  testRequest = {
    body: {},
    query: {
      lat: '39',
      lon: '-76',
      mlra: '136',
      start: '2020-01-01',
      end: '2020-01-02',
      limit: 10,
      offset: 0,
      num: 100,
      location: 'texas', // routeNvm
      year: 2018,
      condition: 'mvm', // routeNvm2Query
      state: 'georgia', // routeCountySpecies
      symbol: 'ABAB,Abac', // routePlants
    },
  };

  // options = 'nomrms';

  testResponse = res;

  testing = true;
  tests = [
    // routeNvm, // slow
    // routeRosetta, // todo
    routeAddresses,
    routeAverages,
    routeCountIndexes,
    routeCountTablesRows,
    routeCounty,
    routeCountySpecies,
    routeDaily,
    routeDatabasesize,
    routeFrost,
    routeGAWeatherStations,
    routeHits,
    routeHourly,
    function routeHourlyPredicted() {
      const tr = JSON.parse(JSON.stringify(testRequest));
      tr.query.predicted = 'true';
      routeHourly(tr);
    },
    routeIndexes,
    routeMLRA,
    routeMLRAErrors,
    routeMlraSpecies,
    routeMlraSpecies2,
    routeMvm,
    routeNvm2,
    routeNvm2Data,
    routeNvm2Query,
    routePlants,
    routePlants2,
    routeVegspecRecords,
    routeVegspecStructure,
    routeTables,
    routeWatershed,
    routeYearly,
  ];

  await testGoogleMapsAPI();
} // routeTest

module.exports = {
  initializeVariables: (req, res, next) => {
    init(req);
    next();
  },
  routeAddresses,
  routeAverages,
  routeCountIndexes,
  routeCountTablesRows,
  routeCounty,
  routeCountySpecies,
  routeDaily,
  routeDatabasesize,
  routeFrost,
  routeGAWeatherStations,
  routeHits,
  routeHourly,
  routeIndexes,
  routeMissingCultivars,
  routeMLRA,
  routeMLRAErrors,
  routeMlraSpecies,
  routeMlraSpecies2,
  routeMvm,
  routeNvm,
  routeNvm2,
  routeNvm2Data,
  routeNvm2Query,
  routeNvm2Update,
  routePlants,
  routePlants2,
  routeVegspecCharacteristics,
  routePlantsEmptyColumns,
  routeVegspecRecords,
  routeVegspecStructure,
  routePlantsTable,
  routeVegspecSaveState,
  routeVegspecDeleteState,
  routeVegspecState,
  routeRosetta,
  routeTables,
  routeTest,
  routeWatershed,
  routeYearly,
};
