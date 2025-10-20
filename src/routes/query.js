import { pool } from 'simple-route';
import { format } from 'sql-formatter';

const googleAPIKey = process.env.GoogleAPI;
const sendResults = (s) => console.log('sendResults', s);

const hits = (ip, url, startTime, results, saveResults = true, email = null) => {
  const time = new Date() - startTime;
  const json = saveResults ? `${JSON.stringify(results)}` : null;

  pool.query(
    `
      INSERT INTO weather.hits (date, ip, query, ms, results, email)
      VALUES (NOW(), $1, $2, $3, $4, $5)
    `,
    [ip, url, time, json, email],
  );
}; // hits

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
  // 'nswrs',
  // 'nlwrs',
  // 'dswrf',
  // 'dlwrf',
  // 'lhtfl',
  // 'shtfl',
  // 'gflux',
  // 'snohf',
  // 'asnow',
  // 'arain',
  // 'evp',
  // 'ssrun',
  // 'bgrun',
  // 'snom',
  // 'avsft',
  // 'albdo',
  // 'weasd',
  // 'snowc',
  // 'snod',
  // 'tsoil',
  // 'soilm1',
  // 'soilm2',
  // 'soilm3',
  // 'soilm4',
  // 'soilm5',
  // 'mstav1',
  // 'mstav2',
  // 'soilm6',
  // 'evcw',
  // 'trans',
  // 'evbs',
  // 'sbsno',
  // 'cnwat',
  // 'acond',
  // 'ccond',
  // 'lai',
  // 'veg',
];

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

/**
 * Fixes column names by replacing specific abbreviations with full column names.
 * @param {string} col - The column name to fix.
 * @param {boolean} [alias=false] - Determines if column aliases should be included.
 * @returns {string} The fixed column name with optional aliases.
 */
const fix = (col, alias) => (
  col.replace(/\btmp\b/i, `air_temperature${alias ? ' as TMP' : ''}`)
    .replace(/\bspfh\b/i, `humidity${alias ? ' as SPFH' : ''}`)
    .replace(/\bpres\b/i, `pressure${alias ? ' as PRES' : ''}`)
    .replace(/\bugrd\b/i, `zonal_wind_speed${alias ? ' as UGRD' : ''}`)
    .replace(/\bvgrd\b/i, `meridional_wind_speed${alias ? ' as VGRD' : ''}`)
    .replace(/\bdlwrf\b/i, `longwave_radiation${alias ? ' as DLWRF' : ''}`)
    .replace(/\bfrain\b/i, `convective_precipitation${alias ? ' as FRAIN' : ''}`)
    .replace(/\bcape\b/i, `potential_energy${alias ? ' as CAPE' : ''}`)
    .replace(/\bpevap\b/i, `potential_evaporation${alias ? ' as PEVAP' : ''}`)
    .replace(/\bapcp\b/i, `precipitation${alias ? ' as APCP' : ''}`)
    .replace(/\bdswrf\b/i, `shortwave_radiation${alias ? ' as DSWRF' : ''}`)
); // fix

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

const validTables = (await pool.query(`
  SELECT t.tablename
  FROM pg_tables t
  LEFT JOIN pg_indexes i ON t.schemaname = i.schemaname AND t.tablename = i.tablename
  WHERE t.tablename LIKE 'nldas%hourly%2023'
  GROUP BY t.tablename
  ORDER BY 1;
`)).rows.map((row) => row.tablename.replace('_2023', ''));

/**
 * Cleans a string by removing specific characters and forbidden keywords.
 * @param {string} s - The string to clean.
 * @returns {string} The cleaned string or 'ERROR' if forbidden keywords are found.
 */
const clean = (s) => {
  const t = decodeURI(s)
    .replace(/\b(doy|day|month|year|growingyear|sum|min|max|avg|count|stddev_pop|stddev_samp|variance|var_pop|var_samp|date|as|abs|and|or)\b/ig, '')
    .replace(/\b(not|between|tmp|air_temperature|spfh|humidity|relative_humidity|pres|pressure|ugrd|zonal_wind_speed|wind_speed|vgrd)\b/ig, '')
    .replace(/\b(meridional_wind_speed|dlwrf|longwave_radiation|frain|convective_precipitation|cape|potential_energy|pevap)\b/ig, '')
    .replace(/\b(potential_evaporation|apcp|precipitation|mrms|dswrf|shortwave_radiation|gdd)\b/ig, '')
    .replace(/["()+\-*/<>,= 0-9.]/ig, '');

  if (t) {
    console.error('*'.repeat(80));
    console.error(t);
    console.error('*'.repeat(80));
    return 'ERROR';
  }
  return s;
}; // clean

const waitForQueries = async () => (
  new Promise((resolve, reject) => {
    const intervalId = setInterval(async () => {
      try {
        const result = await pool.query(`
          SELECT * FROM pg_stat_activity
          WHERE
            client_addr IS NOT NULL
            AND query NOT LIKE '%pg_stat_activity%'
        `);
        if (result.rows.length <= 5) {
          clearInterval(intervalId);
          resolve();
        } else if (result.rows.length > 0) {
          console.log('waitForQueries', result.rows.length);
        }
      } catch (error) {
        clearInterval(intervalId);
        reject(error);
      }
    }, 200);
  })
);

/**
 * Logs a message along with the line number where the debug function is called.
 * @param {string} s - The message to log.
 * @returns {void}
 */
const debug = (s, req, res, status = 200) => {
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
      console.log(err);
      lineNumber = '';
    }

    const result = `
      Line ${lineNumber}
${JSON.stringify(s, null, 2).replace(/\\n/g, '\n')}
    `.trim();

    console.log(result);
    console.log('_'.repeat(process.stdout.columns));

    if (res && !req.testing) {
      res.type('text/plain');
      res.status(status).send(result);
    } else if (res && req.testing) {
      sendResults(req, res, `ERROR\n${result}\n`);
    }
  }
}; // debug

/**
 * Initializes various parameters based on the given request object.
 *
 * @param {Object} req - The request object.
 * @returns {undefined}
 */
const init = async (req, res) => {
  await waitForQueries();
  let location = (req.query.location || '').replace(/[^a-z0-9 ]/ig, '').replace(/\s+/g, ' ').toLowerCase();
  const lats = location ? [] : req.query.lat?.toString().split(',').map((n) => +n);
  const lons = location ? [] : req.query.lon?.toString().split(',').map((n) => +n);
  const options = (req.query.options || '').toLowerCase().split(/\s*,\s*/);

  if (lats.length !== lons.length) {
    // http://localhost/hourly?lat=39.05&lon=-75.87,-76.87&start=2018-11-01&end=2018-11-30&output=html&attributes=precipitation
    res.status(400).send({ error: 'There should be as many lats as there are lons.' });
    return {
      error: true,
    };
  }

  const invalidOption = options.find((col) => !['rect', 'graph', 'gmt', 'utc', 'predicted', 'mrms'].includes(col));
  if (invalidOption) {
    // http://localhost/hourly?lat=20&lon=-76&start=2018-11-01&end=2018-11-30&options=unknown
    res.status(400).send({ error: `Invalid option: ${invalidOption}.  See https://weather.covercrop-data.org/` });
    return {
      error: true,
    };
  }

  const rect = /\brect\b/.test(options) && (req.query.location > '' || lats.length === 2);

  if (/^\/(hourly|daily)/.test(req.url) && lats.length && lons.length) {
    const invalidLocation = lats.some((lat) => lons.some((lon) => (
      !validTables.includes(`nldas_hourly_${parseInt(lat, 10)}_${parseInt(-lon, 10)}`)
    )));

    if (invalidLocation) {
      // http://localhost/hourly?lat=20&lon=-76&start=2018-11-01&end=2018-11-30&attributes=date,precipitation
      res.status(400).send({ error: 'Invalid Location.  See https://ldas.gsfc.nasa.gov/nldas/specifications' });
      return {
        error: true,
      };
    }
  }

  // http://localhost/hourly?lat=39&lon=-76&start=2018-11-01&end=2018-11-30&attributes=date,tmp,apcp,pevap,cape,frain,dlwrf,dswrf,vgrd,ugrd,pres,spfh
  // http://localhost/hourly?lat=39&lon=-76&start=2018-11-01&end=2018-11-30&attributes=date,apcp,precipitation
  // http://localhost/hourly?lat=39&lon=-76&start=2018-11-01&end=2018-11-30&attributes=date,,apcp,,precipitation
  const attr = (req.query.attr || req.query.attributes || '').toLowerCase()
    .split(/\s*,\s*/)
    .map((col) => (
      {
        tmp: 'air_temperature',
        spfh: 'humidity',
        pres: 'pressure',
        ugrd: 'zonal_wind_speed',
        vgrd: 'meridional_wind_speed',
        dswrf: 'shortwave_radiation',
        dlwrf: 'longwave_radiation',
        frain: 'convective_precipitation',
        cape: 'potential_energy',
        pevap: 'potential_evaporation',
        apcp: 'precipitation',
      }[col] || col
    ))
    .filter((col, i, arr) => col && col !== 'date' && !arr.slice(i + 1).includes(col));

  const invalidAttribute = attr.find((col) => (
    ![
      'nswrs', 'nlwrs', 'dswrf', 'dlwrf', 'lhtfl', 'shtfl', 'gflux', 'snohf', 'asnow', 'arain', 'evp', 'ssrun', 'bgrun',
      'snom', 'avsft', 'albdo', 'weasd', 'snowc', 'snod', 'tsoil', 'soilm1', 'soilm2', 'soilm3', 'soilm4', 'soilm5',
      'mstav1', 'mstav2', 'soilm6', 'evcw', 'trans', 'evbs', 'sbsno', 'cnwat', 'acond', 'ccond', 'lai', 'veg',
      'gdd',
      ...parms,
    ].includes(col)
  ));

  if (invalidAttribute) {
    res.status(400).send({ error: `Unknown attribute: ${invalidAttribute}.  See https://weather.covercrop-data.org` });
    return {
      error: true,
    };
  }

  const results = {
    ip: (req.headers?.['x-forwarded-for'] || '').split(',').pop() || req.socket?.remoteAddress,
    output: req.query.explain ? 'json' : req.query.output ?? 'json',
    explain: req.query.explain,
    email: req.query.email || req.headers.origin || req.headers.host,
    lats,
    lons,
    minLat: rect ? Math.min(...lats) : null,
    maxLat: rect ? Math.max(...lats) : null,
    minLon: rect ? Math.min(...lons) : null,
    maxLon: rect ? Math.max(...lons) : null,
    location,
    options,
    rect,
    attr,
    group: req.query.group,
    stats: req.query.stats ? clean(fix(req.query.stats.replace(/[^,]+/g, (s) => `${s} as "${s}"`))) : '',
    where: req.query.where ? clean(fix(req.query.where)).replace(/month/g, 'extract(month from date)') : '',
  };

  const getTimeZone = async () => {
    if (options.includes('gmt') || options.includes('utc') || !results.lats || !results.lons) {
      results.timeOffset = 0;
      return;
    }

    try {
      const results2 = await pool.query(`
        select * from weather.timezone
        where lat=${NLDASlat(results.lats[0])} and lon=${NLDASlon(results.lons[0])}
      `);

      if (results2.rows.length) {
        results.timeOffset = results2.rows[0].rawoffset;
        return;
      }
    } catch (err) {
      debug({
        trigger: 'timezone', lat: results.lats[0], lon: results.lons[0], err,
      }, req, res, 400);
      return;
    }

    try {
      const data = await (await fetch.get(
        `https://maps.googleapis.com/maps/api/timezone/json?location=${NLDASlat(results.lats[0])},${NLDASlon(results.lons[0])}&timestamp=0&key=${googleAPIKey}`,
      )).json();

      if (data.status === 'ZERO_RESULTS') { // Google API can't determine timezone for some locations over water, such as (28, -76)
        results.timeOffset = 0;
        return;
      }

      pool.query(`
        insert into weather.timezone (lat, lon, dstOffset, rawOffset, timeZoneId, timeZoneName)
        values (
          ${NLDASlat(lats[0])}, ${NLDASlon(lons[0])}, ${data.dstOffset}, ${data.rawOffset}, '${data.timeZoneId}', '${data.timeZoneName}'
        )
      `);

      results.timeOffset = data.rawOffset;
    } catch (error) {
      debug(
        {
          trigger: 'Google API timezone',
          lat: lats[0],
          lon: lons[0],
          error,
        },
        req,
        res,
        500,
      );
    }
  }; // getTimeZone

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
  const getLocation = async () => {
    let lresults;

    if (+location) {
      location = `zip ${location}`;
    }
    try {
      lresults = await pool.query(
        'SELECT * FROM weather.addresses WHERE address=$1',
        [location],
      );
    } catch (err) {
      sendResults(req, res, err);
    }

    if (lresults.rows.length) {
      // debug(`Found ${location}`);
      results.lats = [lresults.rows[0].lat];
      results.lons = [lresults.rows[0].lon];
      if (results.rect) {
        results.minLat = Math.min(lresults.rows[0].lat1, lresults.rows[0].lat2);
        results.maxLat = Math.max(lresults.rows[0].lat1, lresults.rows[0].lat2);
        results.minLon = Math.min(lresults.rows[0].lon1, lresults.rows[0].lon2);
        results.maxLon = Math.max(lresults.rows[0].lon1, lresults.rows[0].lon2);
      }
    } else {
      try {
        console.time(`Looking up ${location}`);
        alert('axios');
        const data = await (await(fetch.get(`https://maps.googleapis.com/maps/api/geocode/json?address=${location}&key=${googleAPIKey}`))).json();
        console.timeEnd(`Looking up ${location}`);

        try {
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

          results.lats = [lat];
          results.lons = [lon];

          pool.query(`
            insert into weather.addresses
            (address, lat, lon, lat1, lon1, lat2, lon2)
            values ('${location}', ${lat}, ${lon}, ${lat1}, ${lon1}, ${lat2}, ${lon2})
          `);

          if (rect) {
            results.minLat = Math.min(lat1, lat2);
            results.maxLat = Math.max(lat1, lat2);
            results.minLon = Math.min(lon1, lon2);
            results.maxLon = Math.max(lon1, lon2);
          }

          // if (func) {
          //   func(lats, lons);
          // }
        } catch (ee) {
          debug(ee.message);
        }
      } catch (err) {
        debug({ trigger: 'Google Maps Geocode', location, err }, req, res, 400);
      }
    }
  }; // getLocation

  if (location) {
    await getLocation();
  }

  await getTimeZone();

  // debug(results);

  return results;
}; // init

/**
 * Creates an array of numbers within a specified range.
 *
 * @param {number} start - The start of the range.
 * @param {number} end - The end of the range (inclusive).
 * @returns {Array<number>} - The array of numbers within the specified range.
 */
const range = (start, end) => {
  const result = [];
  for (let i = start; i <= end; i += 1) {
    result.push(i);
  }

  return result;
}; // range

const runQuery = async (req, res, type, start, end, format2, daily) => {
  let years;
  let mrms;
  const {
    ip,
    output,
    explain,
    email,
    attr,
    group,
    where,
    stats,
    lats,
    lons,
    minLat,
    maxLat,
    minLon,
    maxLon,
    location,
    options,
    rect,
    timeOffset,
    error,
  } = await init(req, res);

  if (error) return;

  console.log(JSON.stringify({
    ip,
    output,
    explain,
    lats,
    lons,
    minLat,
    maxLat,
    minLon,
    maxLon,
    location,
    options,
    rect,
    timeOffset,
  }));

  const outputResults = (rows, sq) => {
    if (!rows.length) {
      console.warn('No data found');
      sendResults(req, res, 'No data found');
      return;
    }

    // prevent duplicate rows. screws up LIMIT unfortunately. hopefully unnecessary
    //   let lastJSON;
    //   rows = rows.filter((row) => lastJSON !== (lastJSON = JSON.stringify(row)));

    let s;
    switch (output ? output.toLowerCase() : 'json') {
      case 'csv':
        // http://localhost/hourly?lat=39.032056&lon=-76.873972&start=2018-11-01&end=2018-11-30&output=csv
        s = `${Object.keys(rows[0]).toString()}\n${
          rows.map((r) => Object.keys(r).map((v) => r[v])).join('\n')}`;
        // rows.map(r => Object.values(r).toString()).join('<br>');

        res.set('Content-Type', 'text/csv');
        res.setHeader('Content-disposition', `attachment; filename=${lats}.${lons}.HourlyAverages.csv`);
        res.send(s);
        break;

      case 'html':
        // http://localhost/hourly?lat=39.032056&lon=-76.873972&start=2018-11-01&end=2018-11-30&output=html
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
        if (explain) {
          // http://localhost/hourly?lat=39.032056&lon=-76.873972&start=2018-11-01&end=2018-11-30&output=html&explain=true
          res.set('Content-Type', 'text/plain');
          const responseText = `query:\n ${pretty(sq.trim())}\n\n${rows.map((row) => JSON.stringify(row, null, 2)).join('\n')}`;
          res.send(responseText);
        } else if (req.callback) {
          req.callback(rows);
        } else if (req.testing) {
          sendResults(req, res, 'SUCCESS');
        } else {
          // http://localhost/hourly?lat=39.032056&lon=-76.873972&start=2018-11-01&end=2018-11-30
          res.json(rows);
        }
    }
  }; // outputResults

  const startTime = new Date();

  const cached = (
    await pool.query(
      `
        SELECT results FROM weather.hits
        WHERE query=$1 AND results IS NOT NULL
        LIMIT 1
      `,
      [req.url],
    )
  ).rows[0];

  if (cached) {
    outputResults(JSON.parse(cached.results), '');
    console.log('cached');
    hits(ip, req.url, startTime, null, false, email);
    return;
  }

  let cols;
  let dailyColumns;

  const query = async (offset) => {
    let rtables = {};

    const latlons = [];

    start = new Date(`${start} UTC`);
    start.setSeconds(start.getSeconds() - offset);
    start = start.toISOString();

    end = new Date(`${end} UTC`);
    end.setSeconds(end.getSeconds() - offset);
    end = end.toISOString();

    if (rect) {
      // http://localhost/hourly?lat=39.55,40.03&lon=-75.87,-75.8&start=2018-11-01&end=2018-11-30&output=html&options=graph,rect
      const byy = Math.max(0.125, 0.125 * Math.floor(maxLat - minLat));
      const byx = Math.max(0.125, 0.125 * Math.floor(maxLon - minLon));

      for (let y = minLat; y <= maxLat; y += byy) {
        for (let x = minLon; x <= maxLon; x += byx) {
          rtables[`weather.${type}${Math.trunc(NLDASlat(y))}_${-Math.trunc(NLDASlon(x))}`] = true;
          latlons.push(`'${+NLDASlat(y)}${+NLDASlon(x)}'`);
        }
      }
      rtables = Object.keys(rtables);
    }

    let sq;

    // http://localhost/hourly?lat=39.55,40.03&lon=-75.87,-75.8&start=2018-11-01&end=2018-11-30&output=html&options=graph&attributes=tmp&where=tmp%3C6
    const cond = where ? ` (${where})` : 'true';

    const dateCond = `date BETWEEN '${start}' AND '${end}'`;

    let mrmsResults = [];
    if (mrms) {
      let mrmsQuery = lats.map((lat, i) => (
        years.map((year) => `
          SELECT
            TO_CHAR(date::timestamp + interval '${offset} seconds', '${format2}') AS date,
            precipitation AS mrms,
            ${lat} AS lat,
            ${lons[i]} AS lon
          FROM weather.mrms_${Math.trunc(MRMSround(lat))}_${-Math.trunc(MRMSround(lons[i]))}_${year}
          WHERE lat = ${MRMSround(lat)} AND lon = ${MRMSround(lons[i])} AND ${dateCond}

          UNION ALL
          SELECT
            TO_CHAR(date::timestamp + interval '${offset} seconds', '${format2}') AS date,
            -999 AS mrms,
            ${lat} AS lat,
            ${lons[i]} AS lon
          FROM weather.mrmsmissing
          WHERE ${dateCond}
        `).join('\nUNION ALL\n')
      )).join('\nUNION ALL\n');

      mrmsQuery += `
        ORDER BY date
      `;

      if (req.query.limit) {
        mrmsQuery += `
          LIMIT ${req.query.limit}
        `;
      }

      mrmsResults = (await pool.query(mrmsQuery)).rows;
    }

    const tables = rect ? rtables
      .map((table) => unindent(`
        SELECT
          date, lat AS rlat, lon AS rlon,
          ${attr.length ? fix(attr.join(','), true) : cols}
        FROM (
          ${years.map((year) => (type === 'ha_' ? `SELECT * FROM ${table}` : `SELECT * FROM ${table}_${year}`)).join(' UNION ALL ')}
        ) a
        WHERE
          lat::text || lon IN (${latlons})
          AND date BETWEEN '${start}' AND '${end}'
          AND ${cond}
      `))
      .join(' UNION ALL\n')
      : (lats.map((lat, i) => {
        let mainTable = type === 'nldas_hourly_'
          ? years
            .filter((year) => year !== 'new')
            .map(
              (year) => {
                let table;
                if (req.query.beta) {
                  table = 'weather.nldas_hourly';
                } else {
                  table = `weather.${type}${Math.trunc(NLDASlat(lat))}_${-Math.trunc(NLDASlon(lons[i]))}_${year}`;
                }
                return unindent(`
                  SELECT
                    date,
                    ${cols}
                  FROM ${table}
                  WHERE
                    lat=${NLDASlat(lat)} AND lon=${NLDASlon(lons[i])}
                    AND ${dateCond}
                `);
              },
            ).join(' UNION ALL ')
          : unindent(`
            SELECT date, ${cols}
            FROM weather.${type}${Math.trunc(NLDASlat(lat))}_${-Math.trunc(NLDASlon(lons[i]))}
            WHERE
              lat=${NLDASlat(lat)} AND lon=${NLDASlon(lons[i])}
              AND ${dateCond}
          `);

        const days = (new Date() - new Date(end)) / (1000 * 60 * 60 * 24);
        if (days < 10 && type === 'nldas_hourly_' && (req.query.predicted === 'true' || options.includes('predicted'))) {
          let maxdate = '';
          const year = new Date().getFullYear();

          if (mainTable) {
            mainTable += ' union all ';
            maxdate = `
              date > (
                SELECT MAX(date) FROM (
                  SELECT date FROM weather.${type}${Math.trunc(NLDASlat(lat))}_${-Math.trunc(NLDASlon(lons[i]))}_${year}
                ) a
              )
              AND
            `;
          }

          mainTable += range(+start.slice(0, 4), +end.slice(0, 4) + 1)
            .map((y) => `
              SELECT
                date,
                ${cols}
              FROM (
                SELECT
                  MAKE_TIMESTAMP(${y},
                  EXTRACT(month from date)::integer, EXTRACT(day from date)::integer, EXTRACT(hour from date)::integer, 0, 0) AS date,
                  ${cols}
                FROM weather.ha_${Math.trunc(NLDASlat(lat))}_${-Math.trunc(NLDASlon(lons[i]))}
              ) a
              WHERE
                lat=${NLDASlat(lat)} AND lon=${NLDASlon(lons[i])}
                AND ${maxdate}
                ${dateCond}
            `).join(' UNION ALL ');
        }

        return `
          SELECT ${lat} AS rlat, ${lons[i]} AS rlon, *
          FROM (${mainTable}) a
          ${cond ? `WHERE ${cond}` : ''}
        `;
      }).join(' UNION ALL\n'));

    // console.log(tables);

    // if (req.query.predicted === 'true') {
    //    send(res, tables.replace(/[\n\r]+/g, '<br>')); return;
    // }

    const order = req.query.order
      || `date ${cols.split(/\s*,\s*/).includes('lat') ? ',lat' : ''} ${cols.split(/\s*,\s*/).includes('lon') ? ',lon' : ''}`;

    if (daily) {
      sq = `
        SELECT
          TO_CHAR(date::timestamp + interval '${offset} seconds', '${format2}') AS date,
          ${dailyColumns.replace(/\blat\b/, 'rlat AS lat').replace(/\blon\b/, 'rlon AS lon')}
        FROM (
          SELECT date AS GMT, *
          FROM (${tables}) tables
        ) a
        GROUP BY TO_CHAR(date::timestamp + interval '${offset} seconds', '${format2}'), rlat, rlon
        ORDER BY ${order}
      `;
    } else {
      let other = '';
      const gy = `
        (EXTRACT(year FROM (date::timestamp + interval '${offset} seconds' - interval '5 months')))::text || '-' ||
        (EXTRACT(year FROM (date::timestamp + interval '${offset} seconds' - interval '5 months')) + 1)::text
        AS growingyear, 
      `;

      if (/\bdoy\b/.test(stats)) {
        other += `EXTRACT(doy from date::timestamp + interval '${offset} seconds') AS doy, `;
      }

      if (/\bmonth\b/.test(stats)) {
        other += `EXTRACT(month from date::timestamp + interval '${offset} seconds') As month, `;
      }

      if (/\bgrowingyear\b/.test(stats)) {
        other += gy;
      }

      if (/\byear\b/.test(stats)) {
        other += `EXTRACT(year from date::timestamp + interval '${offset} seconds') AS year, `;
      }

      if (group) {
        other += group
          .replace(/\bdoy\b/g, `EXTRACT(doy from date::timestamp + interval '${offset} seconds') AS doy, `)
          .replace(/\bmonth\b/g, `EXTRACT(month from date::timestamp + interval '${offset} seconds') AS month, `)
          .replace(/\byear\b/g, `EXTRACT(year from date::timestamp + interval '${offset} seconds') As year, `)
          .replace(/\bgrowingyear\b/g, gy);
      }

      sq = `
        SELECT
          ${other} TO_CHAR(date::timestamp + interval '${offset} seconds', '${format2}') AS date,
          ${cols.replace(/\blat\b/, 'rlat AS lat').replace(/\blon\b/, 'rlon AS lon')}
        FROM (
          SELECT DATE AS GMT, *
          FROM (${tables}) tables
        ) a
        ORDER BY ${order}
      `;
      console.log(sq);
    }

    if (stats) {
      sq = `
        SELECT ${group ? `${group}, ` : ''} ${stats}
        FROM (
          ${sq}
        ) alias
        ${group ? `GROUP BY ${group}` : ''}
      `;
    }

    if (req.query.gaws) {
      sq = unindent(`
        SELECT *
        FROM (
          ${sq}
        ) a
        LEFT JOIN (
          SELECT
            ${attr.includes('air_temperature') ? `
              min_air_temperature AS ws_min_air_temperature,
              max_air_temperature AS ws_max_air_temperature,
              avg_air_temperature AS ws_avg_air_temperature,
            ` : ''}
            ${attr.includes('soil_temperature') ? `
              min_soil_temperature_10cm AS ws_min_soil_temperature,
              max_soil_temperature_10cm AS ws_max_soil_temperature,
              avg_soil_temperature_10cm AS ws_avg_soil_temperature,
            ` : ''}
            ${attr.includes('water_temperature') ? `
              min_water_temp AS ws_min_water_temperature,
              max_water_temp AS ws_max_water_temperature,
              avg_water_temp AS ws_avg_water_temperature,
            ` : ''}
            ${attr.includes('pressure') ? `
              min_atmospheric_pressure AS ws_min_pressure,
              max_atmospheric_pressure AS ws_max_pressure,
              avg_atmospheric_pressure AS ws_avg_pressure,
            ` : ''}
            ${attr.includes('relative_humidity') ? `
              min_humidity / 100 AS ws_min_relative_humidity,
              max_humidity / 100 AS ws_max_relative_humidity,
              avg_humidity / 100 AS ws_avg_relative_humidity,
            ` : ''}
            ${attr.includes('dewpoint') ? `
              min_dewpoint AS ws_min_dewpoint,
              max_dewpoint AS ws_max_dewpoint,
              avg_dewpoint AS ws_avg_dewpoint,
            ` : ''}
            ${attr.includes('vapor_pressure') ? `
              min_vapor_pressure AS ws_min_vapor_pressure,
              max_vapor_pressure AS ws_max_vapor_pressure,
              avg_vapor_pressure AS ws_avg_vapor_pressure,
            ` : ''}
            date
          FROM weather.stationdata
          WHERE site=${req.query.gaws}
        ) b
        ON a.date::date = b.date::date
      `);
    }

    if (req.query.limit || req.query.offset) {
      sq = `
        SELECT * FROM (${sq}) a
        LIMIT ${parseInt(req.query.limit, 10) || 100000}
        OFFSET ${parseInt(req.query.offset, 10) || 0}
      `;
    }

    if (mrms) {
      const now = new Date();
      now.setHours(now.getHours() - 2);
      const moredata = new Date(end) > now;
      let maxdate = new Date(start);
      maxdate.setHours(maxdate.getHours() - 2);

      const results = (await pool.query(sq)).rows;
      results.forEach((row1) => {
        if (moredata) {
          if (new Date(row1.date) > maxdate) {
            maxdate = new Date(row1.date);
          }
        }

        const f = mrmsResults.find((row2) => (
          row1.date === row2.date && row1.lat === row2.lat && row1.lon === row2.lon
        ));

        if (f) {
          if (f.mrms !== -999) {
            row1.precipitation = f.mrms;
          }
        } else {
          row1.precipitation = 0;
        }
      });

      if (moredata) {
        const formatDate = (date) => {
          const year = date.getFullYear();
          const month = String(date.getMonth() + 1).padStart(2, '0');
          const day = String(date.getDate()).padStart(2, '0');
          const hours = String(date.getHours()).padStart(2, '0');
          const minutes = String(date.getMinutes()).padStart(2, '0');
          return `${year}-${month}-${day} ${hours}:${minutes}`;
        };

        const enddate = new Date(end);
        lats.forEach((lat, i) => {
          const date = maxdate;
          const lon = lons[i];
          do {
            date.setHours(date.getHours() + 1);
            const f = mrmsResults.find((row2) => (
              new Date(date).toString() === new Date(row2.date).toString() && lat === +row2.lat && lon === +row2.lon
            ));

            let obj = {};
            ['date', ...cols.split(/\s*,\s*/)].forEach((col) => {
              obj[col] = null;
            });

            if (f) {
              if (f.mrms !== -999) {
                obj = {
                  ...obj,
                  date: formatDate(date),
                  lat,
                  lon,
                  precipitation: f.mrms,
                };
              }
            } else {
              obj = {
                ...obj,
                date: formatDate(date),
                lat,
                lon,
                precipitation: 0,
              };
            }

            results.push(obj);
          } while (date < now && date < enddate && results.length < (req.query.limit || 100000));
        });
      }

      hits(ip, req.url, startTime, results, !explain && end && new Date() - new Date(end) > 86400000, email);
      outputResults(results, sq);
    } else if (!(sq.match(/nldas_hourly_\d+_\d+/g) || []).every((table) => validTables.includes(table))) {
      // http://localhost/hourly?lat=20.032056&lon=-76.873972&start=2018-11-01&end=2018-11-30&output=html&attributes=precipitation
      hits(ip, req.url, startTime, [], !explain && end && new Date() - new Date(end) > 86400000, email);
      outputResults([], sq);
    } else {
      const results = (await pool.query(explain ? `EXPLAIN ${sq}` : sq)).rows;
      const nolat = lats.length === 1 && attr.length && !attr.includes('lat');
      const nolon = lons.length === 1 && attr.length && !attr.includes('lon');
      if (nolat || nolon) {
        results.forEach((row) => {
          // http://localhost/hourly?lat=39.03&lon=-76.87&start=2018-11-01&end=2018-11-30&output=html&attributes=precipitation
          // http://localhost/hourly?lat=39.05,40.03&lon=-75.87,-76.87&start=2018-11-01&end=2018-11-30&output=html&attributes=precipitation
          if (nolat) delete row.lat;
          if (nolon) delete row.lon;
        });
      }

      hits(ip, req.url, startTime, results, !explain && end && new Date() - new Date(end) > 86400000, email);
      outputResults(results, sq);
    }

    // exit(sq);
  }; // query

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
    cols = attr.length ? fix(attr.join(','), true) : parms.slice(1).join(', ');
    if (!/\blon\b/.test(cols)) cols = `lon,${cols}`;
    if (!/\blat\b/.test(cols)) cols = `lat,${cols}`;
    dailyColumns = cols;

    if (daily) {
      if (attr.length) {
        dailyColumns = attr.filter((col) => col !== 'gdd') // included automatically if req.query.gddbase
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

        if (!(/\blon\b/).test(dailyColumns)) dailyColumns = `lon,${dailyColumns}`;
        if (!(/\blat\b/).test(dailyColumns)) dailyColumns = `lat,${dailyColumns}`;
        dailyColumns = dailyColumns.replace(/,$/, '');
      } else {
        dailyColumns = `
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
      }

      const { gddbase } = req.query;
      if (gddbase) {
        const mintemp = req.query.gddmin || gddbase;
        const maxtemp = req.query.gddmax || 999;
        dailyColumns += `,
          greatest(0, (
            least(${maxtemp}, max(air_temperature)) + greatest(${mintemp}, least(${maxtemp}, min(air_temperature)))) / 2 - ${gddbase}
          ) as gdd
        `;

        cols = cols.replace(/,?gdd\b/, '');
        if (!cols.includes('air_temperature')) cols += ',air_temperature';
      }
    }
  }; // getColumns

  // const year1 = Math.max(+start.slice(0, 4), 2015);
  const year1 = Math.max(+start.slice(0, 4), 2005);
  let year2 = Math.min(+end.slice(0, 4), new Date().getFullYear());

  if (/12-31/.test(end) && timeOffset !== 0 && year2 < new Date().getFullYear()) {
    // account for local time
    // http://localhost/hourly?lat=39.032056&lon=-76.873972&start=2018-11-01&end=2018-12-31&output=html
    year2 += 1;
  }

  if (req.query.beta) {
    years = [0];
  } else {
    years = range(year1, Math.min(year2, new Date().getFullYear()));

    if (year2 === new Date().getFullYear()) {
      // http://localhost/hourly?lat=39.032056&lon=-76.873972&start=2023-11-01&output=html
      years.push('new');
    }
  }

  mrms = options.includes('mrms') && !explain && !daily && years.length && year2 > 2014 && /hourly/.test(req.url) && !req.query.stats && !rect;

  getColumns();

  mrms = mrms && cols.includes('precipitation');

  query(timeOffset);
}; // runQuery

export default runQuery;