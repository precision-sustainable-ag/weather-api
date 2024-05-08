const { format } = require('sql-formatter');
const axios = require('axios');
const myip = require('ip');
const { pool, googleAPIKey } = require('./pools');
const { debug, sendResults, safeQuery } = require('./database');

const { routeRecords, routeStructure } = require('./vegspec');

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
        }
        console.log('waitForQueries', result.rows.length);
      } catch (error) {
        clearInterval(intervalId);
        reject(error);
      }
    }, 50);
  })
);

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
  const options = req.query.options || '';

  const rect = req.query.options?.includes('rect') && (req.query.location || lats.length === 2);

  let attr = (req.query.attr || req.query.attributes || '').toLowerCase().split(/\s*,\s*/).filter((col) => col);
  if (attr.length && /averages|daily/.test(req.url)) {
    attr = attr.filter((col) => !/frost|nldas/.test(col));
  }

  const results = {
    ip: (req.headers?.['x-forwarded-for'] || '').split(',').pop() || req.socket?.remoteAddress,
    output: req.query.explain ? 'json' : req.query.output ?? 'json',
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
      const data = await (await axios.get(
        // eslint-disable-next-line max-len
        `https://maps.googleapis.com/maps/api/timezone/json?location=${NLDASlat(results.lats[0])},${NLDASlon(results.lons[0])}&timestamp=0&key=${googleAPIKey}`,
      )).data;

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
        const { data } = await axios.get(`https://maps.googleapis.com/maps/api/geocode/json?address=${location}&key=${googleAPIKey}`);
        console.timeEnd(`Looking up ${location}`);

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

const runQuery = async (req, res, type, start, end, format2, daily) => {
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

  const coalesce = (p) => (
    p.map((parm) => {
      switch (parm) {
        case 'date': return 'COALESCE(a.date, b.date) AS date';
        case 'lat': return 'COALESCE(a.lat, b.lat) AS lat';
        case 'lon': return 'COALESCE(a.lon, b.lon) AS lon';
        case 'precipitation': return 'COALESCE(b.precipitation, 0) AS precipitation';
        default: return parm;
      }
    }).join(', ')
  );

  let years;
  let mrms;
  const {
    ip,
    output,
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
  } = await init(req, res);

  console.log({
    ip,
    output,
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
  });

  /** ____________________________________________________________________________________________________________________________________
   * Processes a query and sends the results to the client in the requested format: csv, html, or json (default).
   * Saves the query to the "hits" table, along with date, IP, and runtime.  This can be avoided using the "nosave" parameter.
   * Saves the results to the "queries" table for faster retrieval of repeat queries. Deletes any older than 30 days.
   * If the "explain" parameter exists, sends EXPLAIN details rather than executing the query.
   *
   * @function
   * @param {string} sq - The SQL query to execute.
   * @returns {undefined}
   */
  const sendQuery = (sq) => {
    // pretty(sq);

    const process = (rows) => {
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
          } else if (req.testing) {
            sendResults(req, res, 'SUCCESS');
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
            debug({ sq, error }, req, res, 500);
            return;
          }

          if (!req.query.explain && !req.query.nosave) {
            console.log(!req.query.explain);
            console.log(!req.query.nosave);
            if (
              /averages?/.test(req.originalUrl)
              || (req.query.end && new Date() - new Date(req.query.end) > 86400000)
            ) {
              const jr = JSON.stringify(results2.rows);
              pool.query(`
                INSERT INTO weather.queries (date, url, query, results, ip)
                VALUES (NOW(), '${req.originalUrl}', '${qq}', '${jr}', '${ip}')
              `);
            }

            const time = new Date() - startTime;
            const hits = `
              INSERT INTO weather.hits (date, ip, query, ms)
              VALUES (NOW(), '${ip}', '${req.url}', ${time})
            `;
            pool.query(hits);
          }
          process(results2.rows);
        });
      }
    });
  }; // sendQuery

  let cols;
  const query = (offset) => {
    let byx;
    let byy;
    let rtables = {};
    const latlons = [];

    start = new Date(`${start} UTC`);
    start.setSeconds(start.getSeconds() - offset);
    start = start.toISOString();
    end = new Date(`${end} UTC`);
    end.setSeconds(end.getSeconds() - offset);
    end = end.toISOString();

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
    const cond = where ? ` AND (${where})` : '';
    const dateCond = `date BETWEEN '${start}' AND '${end}'`;

    // console.log(dateCond); process.exit();
    const tables = rect ? rtables
      .map((table) => unindent(`
        SELECT
          lat AS rlat, lon AS rlon,
          ${attr.length ? fix(attr.join(','), true) : cols}
        FROM (
          ${years.map((year) => (type === 'ha_' ? `SELECT * FROM ${table}` : `SELECT * FROM ${table}_${year}`)).join(' UNION ALL ')}
        ) a
        WHERE
          lat::text || lon IN (${latlons})
          AND date BETWEEN '${start}' AND '${end}'
          ${cond}
      `))
      .join(' UNION ALL\n')
      : lats
        .map((lat, i) => {
          let mainTable = type === 'nldas_hourly_'
            ? years.map(
              (year) => unindent(`
                SELECT
                  date,
                  ${cols}
                FROM weather.${type}${Math.trunc(NLDASlat(lat))}_${-Math.trunc(NLDASlon(lons[i]))}_${year}
                WHERE
                  lat=${NLDASlat(lat)} AND lon=${NLDASlon(lons[i])}
                  AND ${dateCond}
              `),
            ).join(' UNION ALL ')
            : unindent(`
              SELECT date, ${cols}
              FROM weather.${type}${Math.trunc(NLDASlat(lat))}_${-Math.trunc(NLDASlon(lons[i]))}
              WHERE
                lat=${NLDASlat(lat)} AND lon=${NLDASlon(lons[i])}
                AND ${dateCond}
            `);

          if (type === 'nldas_hourly_' && (req.query.predicted === 'true' || options.includes('predicted'))) {
            let maxdate = '';
            const year = new Date().getFullYear();

            if (mainTable) {
              mainTable += ' union all ';
              maxdate = `
                date > (
                  SELECT MAX(date) FROM (
                    SELECT date FROM weather.${type}${Math.trunc(NLDASlat(lat))}_${-Math.trunc(NLDASlon(lons[i]))}_new UNION ALL
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

          if (mrms && years.length) {
            // console.log(years); process.exit();
            let mrmsTable = '(';

            mrmsTable += years.map((year) => `
                SELECT date, precipitation AS mrms
                FROM weather.mrms_${Math.trunc(MRMSround(lat))}_${-Math.trunc(MRMSround(lons[i]))}_${year}
                WHERE lat = ${MRMSround(lat)} AND lon = ${MRMSround(lons[i])} AND ${dateCond}
            `).join(' UNION ALL ');

            mrmsTable += `
                UNION ALL

                SELECT b.date, precipitation AS mrms
                FROM weather.mrmsmissing a
                LEFT JOIN (
                  ${mainTable}
                ) b
                USING (date)
                WHERE lat=${NLDASlat(lat)} AND lon=${NLDASlon(lons[i])}
              ) m
            `;

            // originally select distinct:
            const sq2 = `
              SELECT ${lat} AS rlat, ${lons[i]} AS rlon, *
              FROM (
                SELECT 
                  ${coalesce(['date', cols.split(',')])}
                  ${mrms ? ', mrms' : ''}
                FROM (
                  ${mainTable}
                ) a
                FULL JOIN (
                  SELECT date, mrms
                  FROM ${mrmsTable}
                ) b
                USING (date)
              ) alias1
              WHERE
                ${dateCond}
                ${cond}
            `;

            // pretty(sq2);
            return sq2;
          }

          return `
            SELECT ${lat} AS rlat, ${lons[i]} AS rlon, *
            FROM (${mainTable}) a
            WHERE
              lat=${NLDASlat(lat)} AND lon=${NLDASlon(lons[i])}
              AND date between '${start}' AND '${end}'
              ${cond}
          `;
        })
        .join(' UNION ALL\n');

    // if (req.query.predicted === 'true') {
    //    send(res, tables.replace(/[\n\r]+/g, '<br>')); return;
    // }

    const order = req.query.order
      || `1 ${cols.split(/\s*,\s*/).includes('lat') ? ',lat' : ''} ${cols.split(/\s*,\s*/).includes('lon') ? ',lon' : ''}`;

    if (daily) {
      sq = `
        SELECT
          TO_CHAR(date::timestamp + interval '${offset} seconds', '${format2}') AS date,
          ${cols.replace(/\blat\b/, 'rlat AS lat').replace(/\blon\b/, 'rlon AS lon')}
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

      if (mrms) {
        cols = cols.replace(/\bprecipitation\b/, 'COALESCE(mrms, 0) AS precipitation');
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

    console.log(pretty(sq)); // process.exit();

    sendQuery(sq);
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
    if (daily) {
      if (attr.length) {
        cols = attr.filter((col) => col !== 'gdd') // included automatically if req.query.gddbase
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
      cols = attr.length ? fix(attr.join(','), true) : parms.slice(1).join(', ');
      if (!cols.includes('lon')) cols = `lon,${cols}`;
      if (!cols.includes('lat')) cols = `lat,${cols}`;
    }
  }; // getColumns

  // const year1 = Math.max(+start.slice(0, 4), 2015);
  const year1 = Math.max(+start.slice(0, 4), 2005);
  let year2 = Math.min(+end.slice(0, 4), new Date().getFullYear());

  if (/12-31/.test(end) && timeOffset !== 0) {
    // account for local time
    year2 += 1;
  }

  years = range(year1, Math.min(year2, new Date().getFullYear()));

  if (year2 === new Date().getFullYear()) {
    years.push('new');
  }

  mrms = year2 > 2014 && /hourly|daily/.test(req.url) && !/nomrms/.test(options) && !req.query.stats;

  getColumns();

  mrms = mrms && cols.includes('precipitation');

  query(timeOffset);
}; // runQuery

const routeHourly = (req, res) => {
  const start = req.query.start || '2000-01-01';
  const end = req.query.end ? req.query.end + (/:/.test(req.query.end) ? '' : ' 23:59')
    : '2099-12-31 23:59';

  req.url = req.url || 'hourly'; // in case testing

  runQuery(req, res, 'nldas_hourly_', start, end, 'YYYY-MM-DD HH24:MI');
}; // routeHourly

const routeDaily = (req, res) => {
  const start = req.query.start || '2000-01-01';
  const end = req.query.end ? `${req.query.end} 23:59` : '2099-12-31 23:59';

  req.url = req.url || 'daily'; // in case testing

  runQuery(req, res, 'nldas_hourly_', start, end, 'YYYY-MM-DD', true);
}; // routeDaily

const routeAverages = (req, res) => {
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
  const sql = `
    ${req.query.explain ? `EXPLAIN ${sq}` : sq}
    LIMIT ${req.query.limit || 100000}
    OFFSET ${req.query.offset || 0}
  `;

  pool.query(
    sql,
    (err, results) => {
      if (err) {
        debug(err, req, res, 500);
      } else if (req.testing) {
        sendResults(req, res, 'SUCCESS');
      } else {
        res.json(results.rows);
      }
    },
  );
}; // queryJSON

const routeGAWeatherStations = (req, res) => {
  queryJSON(req, res, 'select * from weather.stations order by County');
}; // routeGAWeatherStations

const routeAddresses = (req, res) => {
  queryJSON(req, res, 'select * from weather.addresses order by address');
}; // routeAddresses

const routeIndexes = (req, res) => {
  queryJSON(req, res, 'select * from pg_indexes where tablename not like \'pg%\' order by indexname');
}; // routeIndexes

const routeTables = (req, res) => {
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

const routeCountTablesRows = (req, res) => {
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

const routeCountIndexes = (req, res) => {
  queryJSON(req, res, `
    select count(*) as indexes
    from pg_indexes
    where schemaname = 'weather'
  `);
}; // routeCountIndexes

const routeDatabasesize = (req, res) => {
  queryJSON(req, res, `
    select pg_size_pretty(pg_database_size('postgres')) as size
  `);
}; // routeDatabasesize

const routeHits = (req, res) => {
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

const routeMvm = (req, res) => {
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
      sendResults(req, res, `ERROR:<br>${sq.replace(/\n/g, '<br>').replace(/ /g, '&nbsp;')}`);
    } else if (req.testing) {
      sendResults(req, res, 'SUCCESS');
    } else {
      sendResults(req, res, JSON.stringify(results.rows));
    }
  });
}; // routeMvm

const routeNvm = async (req, res) => {
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
        sendResults(req, res, `ERROR:<br>${sq1.replace(/\n/g, '<br>').replace(/ /g, '&nbsp;')}`);
        return;
      }
      s += `${data(results)}<hr>`;

      if (req.query.explain) {
        sq2 = `explain ${sq2}`;
      }

      pool.query(sq2, (e, results2) => {
        if (e) {
          sendResults(req, res, `ERROR:<br>${sq2.replace(/\n/g, '<br>').replace(/ /g, '&nbsp;')}`);
        } else if (req.testing) {
          sendResults(req, res, 'SUCCESS');
        } else {
          s += data(results2);
          sendResults(req, res, s);
        }
      });
    });
  }; // NVMprocess

  NVMprocess();
}; // routeNvm

const routeNvm2 = (req, res) => {
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
            sendResults(req, res, err);
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
                    sendResults(req, res, e);
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

                      sendResults(req, res, s);
                    } catch (error) {
                      sendResults(req, res, error.message);
                    }
                  }
                },
              );
            } catch (ee) {
              sendResults(req, res, ee.message);
            }
          }
        },
      );
    }; // runNvmQuery

    pool.query(
      `select data from weather.nvm2 where lat = ${lat} and lon = ${lon} and year = ${year}`,
      (err, results) => {
        if (err) {
          sendResults(req, res, err);
        } else if (req.testing) {
          sendResults(req, res, 'SUCCESS');
        } else if (results.rowCount) {
          sendResults(req, res, results.rows[0].data);
        } else {
          runNvmQuery();
        }
      },
    );
  } catch (ee) {
    sendResults(req, res, ee.message);
  }
}; // routeNvm2

const routeNvm2Data = (req, res) => {
  pool.query(
    'select distinct lat, lon, year from weather.nvm2',
    (err, results) => {
      if (err) {
        sendResults(req, res, 'ERROR');
      } else if (req.testing) {
        sendResults(req, res, 'SUCCESS');
      } else {
        sendResults(req, res, JSON.stringify(results.rows));
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
  sendResults(req, res, sq);
}; // routeNvm2Update

const routeNvm2Query = (req, res) => {
  try {
    const sq = `select lat, lon from weather.nvm2
              where ${req.query.condition.replace(/select|insert|update|drop|delete/ig, '')}
              order by lat, lon
             `;

    pool.query(
      sq,
      (err, results) => {
        if (err) {
          sendResults(req, res, err);
        } else if (req.testing) {
          sendResults(req, res, 'SUCCESS');
        } else if (results.rowCount) {
          sendResults(req, res, JSON.stringify(results.rows));
        }
      },
    );
  } catch (ee) {
    console.error(ee.message);
  }
}; // routeNvm2Query

const routeRosetta = (req, res) => {
  axios.post(
    'https://www.handbook60.org/api/v1/rosetta/1',
    {
      soildata: req.body.soildata,
    },
  ).then((data) => {
    sendResults(req, res, data.data);
  });
}; // routeRosetta

const routeWatershed = async (req, res) => {
  const { location, lats, lons } = await init(req, res);

  const query = (sq) => {
    // pretty(sq);
    pool.query(
      sq,
      (error, results) => {
        if (error) {
          debug({ sq, error }, req, res, 500);
        } else if (results.rows.length) {
          sendResults(req, res, results.rows.map((row) => {
            delete row.geometry;
            return row;
          }));
        } else {
          sendResults(req, res, {});
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
    latLon();
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
    latLon();
  }
}; // routeWatershed

const routeCounty = async (req, res) => {
  const {
    lats,
    lons,
  } = await init(req, res);

  const query = (sq) => {
    pretty(sq);
    pool.query(
      sq,
      (err, results) => {
        if (err) {
          debug(err, req, res, 500);
        } else if (results.rows.length) {
          sendResults(req, res, results.rows.map((row) => {
            delete row.geometry;
            return row;
          }));
        } else {
          sendResults(req, res, {});
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
    query(`
      SELECT distinct ${attributes}
      FROM counties
      WHERE ST_Contains(geometry::geometry, ST_Transform(ST_SetSRID(ST_GeomFromText('POINT(${lons[0]} ${lats[0]})'), 4326), 4269))
    `);
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

  latLon();
}; // routeCounty

const routeHardinessZone = async (req, res) => {
  const {
    lat,
    lon,
    polygon,
  } = req.query;

  // (ST_AsGeoJSON(ST_Multi(geometry))::jsonb->\'coordinates\') as polygonarray

  const sq = `
    SELECT
      id, gridcode, zone, trange, zonetitle
      ${polygon?.toString() === 'true' ? ', (ST_AsGeoJSON(ST_Multi(geometry))::jsonb->\'coordinates\') as polygonarray' : ''}
    FROM hardiness_zones
    WHERE ST_Intersects(
      'SRID=4326;POINT(${lon} ${lat})'::geometry,
      geometry
    );
  `;

  // console.log(sq);

  const results = await pool.query(sq);

  if (results.rows.length) {
    res.send(results.rows[0]);
  } else {
    res.send({ error: 'No results' });
  }
}; // routeHardinessZone

const routeMLRA = async (req, res) => {
  const {
    lats,
    lons,
  } = await init(req, res);
  console.log(lats, lons);

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
          debug(err, req, res, 500);
        } else if (results.rows.length) {
          sendResults(req, res, results.rows.map((row) => {
            delete row.geometry;
            return row;
          }));
        } else {
          sendResults(req, res, {});
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

  latLon();
}; // routeMLRA

const routeFrost = async (req, res) => {
  const {
    lats,
    lons,
  } = await init(req, res);

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
        debug(err, req, res, 500);
      } else if (results.rows.length) {
        sendResults(req, res, results.rows[0]);
      } else {
        sendResults(req, res, {});
      }
    },
  );
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
const routeYearly = async (req, res) => {
  const { lats, lons } = await init(req, res);

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
      min(sum_precipitation) AS min_precipitation,
      max(sum_precipitation) AS max_precipitation,
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

  // console.log(sq);

  // Executing the SQL query.
  pool.query(
    sq,
    (err, results) => {
      if (err) {
        debug(err, req, res, 500);
      } else {
        sendResults(req, res, results.rows);
      }
    },
  );
}; // routeYearly

const routeTest = async (req, res) => {
  const testRequest = {
    testing: true,
    testResponse: res,
    headers: req.headers,
    socket: req.socket,
    body: {},
    query: {
      nosave: true,
      // explain: true,
      lat: '39',
      lon: '-76',
      mlra: '136',
      start: '2020-01-01',
      end: '2020-01-02',
      limit: 10,
      offset: 0,
      num: 100,
      // location: 'texas', // routeNvm
      location: '',
      year: 2018,
      condition: 'mvm', // routeNvm2Query
    },
  };

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
    sendResults(testRequest, res, results);
  } // GoogleMapsAPI

  // options = 'nomrms';

  testRequest.tests = [
    // routeNvm, // slow
    // routeRosetta, // todo
    routeRecords,
    routeStructure,

    routeAddresses,
    routeAverages,
    routeCountIndexes,
    routeCountTablesRows,
    routeCounty,
    routeDaily,
    routeDatabasesize,
    routeFrost,
    routeGAWeatherStations,
    routeHits,
    routeHourly,
    function routeHourlyPredicted() {
      const tr = { ...testRequest };
      tr.query.predicted = 'true';
      routeHourly(tr, res);
    },
    routeIndexes,
    routeMLRA,
    routeMvm,
    routeNvm2,
    routeNvm2Data,
    routeNvm2Query,
    routeTables,
    routeWatershed,
    routeYearly,
  ];

  await testGoogleMapsAPI();
}; // routeTest

const elevations = {};
const routeElevation = async (req, res) => {
  const lat = (+req.query.lat).toFixed(6);
  const lon = (+req.query.lon).toFixed(6);
  const latLon = `${lat} ${lon}`;

  if (!elevations[latLon]) {
    try {
      const url = `https://api.open-elevation.com/api/v1/lookup?locations=${lat},${lon}`;
      // eslint-disable-next-line prefer-destructuring
      elevations[latLon] = (await (
        await fetch(url)
      ).json()).results[0];
      console.log('fetched', url, elevations[latLon]);
    } catch {
      res.status(500).send({
        latitude: lat, longitude: lon, elevation: 'NA', error: 'failed',
      });
    }
  }

  res.send(elevations[latLon]);
}; // routeElevation

module.exports = {
  routeAddresses,
  routeAverages,
  routeCountIndexes,
  routeCountTablesRows,
  routeCounty,
  routeDaily,
  routeDatabasesize,
  routeElevation,
  routeFrost,
  routeGAWeatherStations,
  routeHardinessZone,
  routeHits,
  routeHourly,
  routeIndexes,
  routeMLRA,
  routeMvm,
  routeNvm,
  routeNvm2,
  routeNvm2Data,
  routeNvm2Query,
  routeNvm2Update,
  routeRosetta,
  routeTables,
  routeTest,
  routeWatershed,
  routeYearly,
};
