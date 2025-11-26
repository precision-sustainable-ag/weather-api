import { pool } from 'simple-route';
import { format } from 'sql-formatter';

const googleAPIKey = process.env.GoogleAPI;

const testing = true;

// const { rows } = pool.query('SELECT DISTINCT lat, lon FROM weather'); // !!! new server

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
];

const mosaic = [
  'nswrs',
  'nlwrs',
  'dswrf',
  'dlwrf',
  'lhtfl',
  'shtfl',
  'gflux',
  'snohf',
  'asnow',
  'arain',
  'evp',
  'ssrun',
  'bgrun',
  'snom',
  'avsft',
  'albdo',
  'weasd',
  'snowc',
  'snod',
  'tsoil',
  'soilm1',
  'soilm2',
  'soilm3',
  'soilm4',
  'soilm5',
  'mstav1',
  'mstav2',
  'soilm6',
  'evcw',
  'trans',
  'evbs',
  'sbsno',
  'cnwat',
  'acond',
  'ccond',
  'lai',
  'veg',
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
  // console.log(result);
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
// const MRMSround = (n) => (Math.round((n - 0.005) * 100) / 100 + 0.005).toFixed(3);
const MRMSround = (n) => 5 + 10 * Math.round(100 * n - 0.5);

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

  if (t.length) {
    console.error('*'.repeat(80));
    console.error(t);
    console.error('*'.repeat(80));
    return 'ERROR';
  }
  return s;
}; // clean

// eslint-disable-next-line no-unused-vars
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
 * Initializes various parameters based on the given request object.
 *
 * @param {Object} req - The request object.
 * @returns {undefined}
 */
const init = async (inputs) => {
  let { options } = inputs;

  const {
    req, reply,
    lat, lon,
    explain, email, output,
    group, stats, where, url,
  } = inputs;

  const attributes = inputs.attributes?.replace('mosaic', mosaic).replace('nldas', parms);

  // await waitForQueries();
  const lats = lat?.toString().split(',').map((n) => +n);
  const lons = lon?.toString().split(',').map((n) => +n);

  const rect = /\brect\b/.test(options) && lats.length === 2;

  options = options?.toLowerCase().split(/\s*,\s*/) || [];

  if (lats.length !== lons.length) {
    // http://localhost/hourly?lat=39.05&lon=-75.87,-76.87&start=2018-11-01&end=2018-11-30&output=html&attributes=precipitation
    reply.code(400).send({ error: 'There should be as many latitudes as longitudes.' });
    return false;
  }

  const invalidOption = options.find((col) => !['rect', 'graph', 'gmt', 'utc', 'predicted', 'mrms'].includes(col));
  if (invalidOption) {
    // http://localhost/hourly?lat=39.05&lon=-75.87,-76.87&start=2018-11-01&end=2018-11-30&output=html&option=hmm
    reply.code(400).send({ error: `Invalid option: ${invalidOption}.  See https://weather.covercrop-data.org/` });
    return false;
  }

  // http://localhost/hourly?lat=39&lon=-76&start=2018-11-01&end=2018-11-30&attributes=date,tmp,apcp,pevap,cape,frain,dlwrf,dswrf,vgrd,ugrd,pres,spfh
  // http://localhost/hourly?lat=39&lon=-76&start=2018-11-01&end=2018-11-30&attributes=date,apcp,precipitation
  // http://localhost/hourly?lat=39&lon=-76&start=2018-11-01&end=2018-11-30&attributes=date,,apcp,,precipitation
  const attr = (attributes || '').toLowerCase()
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
      ...parms,
      ...mosaic,
      'gdd',
    ].includes(col)
  ));

  if (invalidAttribute) {
    // http://localhost/hourly?lat=39.05&lon=-75.87&start=2018-11-01&end=2018-11-30&output=html&attributes=precipitation,unknown
    reply.code(400).send({ error: `Unknown attribute: ${invalidAttribute}.  See https://weather.covercrop-data.org` });
    return false;
  }

  const results = {
    ip: (req.headers?.['x-forwarded-for'] || '').split(',').pop() || req.socket?.remoteAddress,
    output: explain ? 'json' : output ?? 'json',
    explain,
    email: email || req.headers?.origin || req.headers?.host,
    lats,
    lons,
    minLat: rect ? Math.min(...lats) : null,
    maxLat: rect ? Math.max(...lats) : null,
    minLon: rect ? Math.min(...lons) : null,
    maxLon: rect ? Math.max(...lons) : null,
    options,
    rect,
    attr,
    group,
    stats: stats ? clean(fix(stats.replace(/[^,]+/g, (s) => `${s} as "${s}"`))) : '',
    where: where ? clean(fix(where)).replace(/month/g, 'extract(month from date)') : '',
    daily: url === 'daily',
  };

  if (results.stats === 'ERROR') {
    inputs.reply.code(400).send({ error: `Don't understand stats=${inputs.stats}` });
    return false;
  }

  if (results.where === 'ERROR') {
    inputs.reply.code(400).send({ error: `Don't understand where=${inputs.where}` });
    return false;
  }  

  const getTimeZone = async () => {
    const { rows } = await pool.query(`
      SELECT MIN(date - interval '1 hour') AS mindate, MAX(date + interval '1 hour') AS maxdate
      FROM weather
      WHERE lat=30 AND lon=-115
    `);

    results.mindate = new Date(`${rows[0].mindate} UTC`);
    results.maxdate = new Date(`${rows[0].maxdate} UTC`);

    if (options.includes('gmt') || options.includes('utc') || !results.lats || !results.lons) {
      results.timeOffset = 0;
      return;
    }

    try {
      const results2 = await pool.query(`
        SELECT * FROM timezone
        WHERE lat=${NLDASlat(results.lats[0])} AND lon=${NLDASlon(results.lons[0])}
      `);

      if (results2.rows.length) {
        results.timeOffset = results2.rows[0].rawoffset;
        return;
      }
    } catch (err) {
      reply.code(400).send({ error: 'timezone', lat: results.lats[0], lon: results.lons[0], err });
      return false;
    }

    try {
      const data = await (await fetch(
        `https://maps.googleapis.com/maps/api/timezone/json?location=${NLDASlat(results.lats[0])},${NLDASlon(results.lons[0])}&timestamp=0&key=${googleAPIKey}`,
      )).json();

      if (data.status === 'ZERO_RESULTS') { // Google API can't determine timezone for some locations over water, such as (28, -76)
        results.timeOffset = 0;
        return;
      }

      pool.query(`
        INSERT INTO timezone
        (lat, lon, dstOffset, rawOffset, timeZoneId, timeZoneName)
        VALUES (${NLDASlat(lats[0])}, ${NLDASlon(lons[0])}, ${data.dstOffset}, ${data.rawOffset}, '${data.timeZoneId}', '${data.timeZoneName}')
      `);

      results.timeOffset = data.rawOffset;
    } catch (err) {
      console.error('500 this');
      reply.code(500).send({ error: 'Google API timezone', lat: results.lats[0], lon: results.lons[0], err });
      return false;
    }
  }; // getTimeZone

  await getTimeZone();

  return results;
}; // init

/** ____________________________________________________________________________________________________________________________________
 * Gets the latitude and longitude coordinates of a location using the Google Maps API or the database.
 * (New locations are added to the database.)
 * If `location` is a valid ZIP code, it will be automatically converted to "zip <code>".
 * If `rect` is `true`, also calculates the bounding box (minLat, maxLat, minLon, maxLon) for the location.
 *
 */
const getLocation = async (location, results, rect) => {
  if (+location) {
    location = `zip ${location}`;
  }

  const lresults = await pool.query(
    'SELECT * FROM addresses WHERE address=$1',
    [location],
  );

  if (lresults.rows.length) {
    results.lats = [lresults.rows[0].lat];
    results.lons = [lresults.rows[0].lon];
    if (rect) {
      results.minLat = Math.min(lresults.rows[0].lat1, lresults.rows[0].lat2);
      results.maxLat = Math.max(lresults.rows[0].lat1, lresults.rows[0].lat2);
      results.minLon = Math.min(lresults.rows[0].lon1, lresults.rows[0].lon2);
      results.maxLon = Math.max(lresults.rows[0].lon1, lresults.rows[0].lon2);
    }
  } else {
    try {
      console.time(`Looking up ${location}`);
      const data = await (await(fetch(`https://maps.googleapis.com/maps/api/geocode/json?address=${location}&key=${googleAPIKey}`))).json();
      console.timeEnd(`Looking up ${location}`);

      try {
        const lat = data.results[0].geometry.location.lat;
        const lon = data.results[0].geometry.location.lng;
        const lat1 = data.results[0].geometry.viewport.northeast.lat;
        const lon1 = data.results[0].geometry.viewport.northeast.lng;
        const lat2 = data.results[0].geometry.viewport.southwest.lat;
        const lon2 = data.results[0].geometry.viewport.southwest.lng;

        results.lats = [lat];
        results.lons = [lon];

        pool.query(`
          INSERT INTO addresses
          (address, lat, lon, lat1, lon1, lat2, lon2)
          VALUES ('${location}', ${lat}, ${lon}, ${lat1}, ${lon1}, ${lat2}, ${lon2})
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
        console.log(ee.message);
      }
    } catch (err) {
      console.log({ trigger: 'Google Maps Geocode', location, err });
    }
  }
}; // getLocation

const runQuery = async (inputs) => {
  let mrms;
  const initialized = await init(inputs);

  if (!initialized) {
    return;
  }

  const {
    explain,
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
    options,
    rect,
    timeOffset,
    error,
    mindate,
    maxdate,
    daily,
  } = initialized;

  const {
    req, limit, offset, type, format2,
    gddbase, gddmax, gddmin,
  } = inputs;

  let {
    start, end, order,
  } = inputs;

  // console.log({
  //   ip,
  //   output,
  //   explain,
  //   email,
  //   attr,
  //   group,
  //   where,
  //   stats,
  //   lats,
  //   lons,
  //   minLat,
  //   maxLat,
  //   minLon,
  //   maxLon,
  //   options,
  //   rect,
  //   timeOffset,
  //   error,
  //   limit,
  //   type,
  //   format2,
  //   daily,
  //   order,
  // });

  if (error) return;

  const outputResults = (rows, sq) => {
    if (explain) {
      // http://localhost/hourly?lat=39.032056&lon=-76.873972&start=2018-11-01&end=2018-11-30&output=html&explain=true
      const responseText = `query:\n ${pretty(sq.trim())}\n\n${rows.map((row) => JSON.stringify(row, null, 2)).join('\n')}`;
      return responseText;
    } else if (req.callback) { // !!!
      req.callback(rows);
    } else {
      // http://localhost/hourly?lat=39.032056&lon=-76.873972&start=2018-11-01&end=2018-11-30
      rows = rows.map((row) => {
        if (row.date) {
          if (format2 === 'yyyy-mm-dd HH:00') {
            row.date = new Date(new Date(`${row.date}`).getTime() + timeOffset * 1000).toISOString().replace('T', ' ').slice(0, 16);
          } else if (format2 === 'yyyy-mm-dd') {
            // http://localhost/daily?email=jd@ex.com&lat=35.5&lon=-80.8&start=2018-11-01&end=2018-11-30&output=html
            row.date = new Date(new Date(`${row.date}`).getTime()).toISOString().slice(0, 10);
          } else if (format2 === 'MM-DD HH24:00') {
            row.date = new Date(new Date(`${row.date}`).getTime() + timeOffset * 1000).toISOString().replace('T', ' ').slice(5, 16);
          }
        }
        return row;
      });

      return rows;
    }
  }; // outputResults

  let cols;
  let dailyColumns;

  const query = async (timeOffset) => {
    start = new Date(`${start} UTC`);
    start.setSeconds(start.getSeconds() - timeOffset);
    start = start.toISOString();

    end = new Date(`${end} UTC`);
    end.setSeconds(end.getSeconds() - timeOffset);
    end = end.toISOString();

    const showPredicted = type !== 'averages' && (new Date(start) < mindate || new Date(end) > maxdate);

    if (rect) {
      // http://localhost/hourly?lat=39.55,40.03&lon=-75.87,-75.8&start=2018-11-01&end=2018-11-30&output=html&options=graph,rect
      const byy = Math.max(0.125, 0.125 * Math.floor(maxLat - minLat));
      const byx = Math.max(0.125, 0.125 * Math.floor(maxLon - minLon));
      lats.length = 0;
      lons.length = 0;

      for (let y = minLat; y <= maxLat; y += byy) {
        for (let x = minLon; x <= maxLon; x += byx) {
          lats.push(NLDASlat(y));
          lons.push(NLDASlon(x));
        }
      }
    }

    let sq;

    // http://localhost/hourly?lat=39.55,40.03&lon=-75.87,-75.8&start=2018-11-01&end=2018-11-30&output=html&options=graph&attributes=tmp&where=tmp%3C6
    const cond = where ? ` (${where})` : 'true';

    const date1 = start.slice(0, -1);
    const date2 = end.slice(0, -1);

    let mrmsResults = [];
    if (mrms) { // !!! mrmsmissing
      // Math.floor logic causes PostgreSQL to use the correct index
      let mrmsQuery = lats
        .map((lat, i) => (`
          SELECT
            date::timestamptz AS date,
            precipitation
          FROM mrms
          WHERE
            lat >= ${Math.floor(lat)} AND lat < ${Math.floor(lat) + 1}
            AND lon >= ${Math.floor(lons[i])} AND lon < ${Math.floor(lons[i]) + 1}
            AND round(lat * 1000) = ${MRMSround(lat)}
            AND round(lon * 1000) = ${MRMSround(lons[i])}
            AND date BETWEEN '${date1}'::timestamptz AND '${date2}'::timestamptz
        `))
        .join('\nUNION ALL\n');
      
      mrmsQuery += `
        ORDER BY date
      `;
      // console.log(mrmsQuery); throw '';

      if (limit) {
        mrmsQuery += `
          LIMIT ${limit}
        `;
      }

      console.log(mrmsQuery);
      mrmsResults = (await pool.query(mrmsQuery)).rows;
      console.log(mrmsResults);
    }

    const tables =
      (lats.map((lat, i) => {
        let mainTable;
        
        if (
          type === 'hourly' && (
            (new Date(start) >= mindate || new Date(end) <= maxdate)
            || (new Date(start) <= mindate && new Date(end) >= maxdate)
          )
        ) {
          mainTable = unindent(`
            SELECT date, ${cols} ${showPredicted ? ', FALSE AS predicted' : ''}
            FROM nldas(${NLDASlat(lat)}, ${NLDASlon(lons[i])}, '${date1}', '${date2}')
          `);

          if (new Date(date1) < mindate) {
            const d = new Date(end) >= mindate ? mindate.toISOString() : date2;
            mainTable = unindent(`
              SELECT date, ${cols}, TRUE AS predicted
              FROM averages(${NLDASlat(lat)}, ${NLDASlon(lons[i])}, '${date1}'::timestamptz, '${d}'::timestamptz)
              UNION ALL
              ${mainTable}
            `);
          }

          if (new Date(date2) > maxdate) {
            const d = new Date(start) <= maxdate ? maxdate.toISOString() : date1;
            mainTable = unindent(`
              ${mainTable}
              UNION ALL
              SELECT date, ${cols}, TRUE AS predicted
              FROM averages(${NLDASlat(lat)}, ${NLDASlon(lons[i])}, '${d}'::timestamptz, '${date2}'::timestamptz)
            `);
          }
        } else {
          mainTable = unindent(`
            SELECT date, ${cols}, TRUE AS predicted
            FROM averages(${NLDASlat(lat)}, ${NLDASlon(lons[i])}, '${date1}', '${date2}')
          `);
        }

        return `
          SELECT ${lat} AS rlat, ${lons[i]} AS rlon, *
          FROM (${mainTable}) a
          ${cond ? `WHERE ${cond}` : ''}
        `;
      }).join(' UNION ALL\n'));

    // console.log(tables);

    order = order || `date ${cols.split(/\s*,\s*/).includes('lat') ? ',lat' : ''} ${cols.split(/\s*,\s*/).includes('lon') ? ',lon' : ''}`;

    if (daily) {
      sq = `
        SELECT
          TO_CHAR(date::timestamp + interval '${timeOffset} seconds', '${format2}') AS date,
          ${dailyColumns.replace(/\blat\b/, 'rlat AS lat').replace(/\blon\b/, 'rlon AS lon')}
        FROM (
          SELECT date AS GMT, *
          FROM (${tables}) tables
        ) a
        GROUP BY TO_CHAR(date::timestamp + interval '${timeOffset} seconds', '${format2}'), rlat, rlon
        ORDER BY ${order}
      `;
    } else {
      let other = '';
      const gy = `
        (EXTRACT(year FROM (date::timestamp + interval '${timeOffset} seconds' - interval '7 months')))::text || '-' ||
        (EXTRACT(year FROM (date::timestamp + interval '${timeOffset} seconds' - interval '7 months')) + 1)::text
        AS growingyear,
      `;

      if (group) {
        other += group
          .replace(/\bdoy\b/g, `EXTRACT(doy from date::timestamp + interval '${timeOffset} seconds') AS doy, `)
          .replace(/\bmonth\b/g, `EXTRACT(month from date::timestamp + interval '${timeOffset} seconds') AS month, `)
          .replace(/\byear\b/g, `EXTRACT(year from date::timestamp + interval '${timeOffset} seconds') AS year, `)
          .replace(/\bgrowingyear\b/g, gy);
      }

      sq = `
        SELECT
          ${other}
          date,
          ${cols.replace(/\blat\b/, 'rlat AS lat').replace(/\blon\b/, 'rlon AS lon')}
          ${showPredicted ? ', predicted' : ''}
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
        ${group ? `ORDER BY ${group}` : ''}
      `;
    }

    if (limit || offset) {
      sq = `
        SELECT * FROM (${sq}) a
        LIMIT ${parseInt(limit, 10) || 100000}
        OFFSET ${parseInt(offset, 10) || 0}
      `;
    }

    if (testing) console.log(sq);

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
          new Date(row1.date).getTime() === new Date(row2.date).getTime()
        ));

        if (f) {
          row1.precipitation = f.precipitation;
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
          } while (date < now && date < enddate && results.length < (limit || 100000));
        });
      }

      return outputResults(results, sq);
    } else {
      const client = await pool.connect();
      let results = [];
      try {
        await client.query('BEGIN');
        await client.query('SET LOCAL max_parallel_workers_per_gather = 8');
        await client.query('SET LOCAL parallel_setup_cost = 0');
        await client.query('SET LOCAL parallel_tuple_cost = 0');
        await client.query('SET enable_bitmapscan = off;');
        await client.query('SET enable_indexonlyscan = on;');
        await client.query(`SET min_parallel_index_scan_size = '0';`);

        results = (await client.query(explain ? `EXPLAIN (ANALYZE, BUFFERS) ${sq}` : sq)).rows;
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
      } catch (e) {
        await client.query('ROLLBACK');
        throw e;
      } finally {
        client.release();
      }

      return outputResults(results, sq);
    }
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
        dailyColumns = attr.filter((col) => col !== 'gdd') // included automatically if gddbase
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

      if (gddbase) {
        const mintemp = gddmin || gddbase;
        const maxtemp = gddmax || 999;
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
  let year2 = Math.min(+end.slice(0, 4), new Date().getFullYear());

  if (/12-31/.test(end) && timeOffset !== 0 && year2 < new Date().getFullYear()) {
    // account for local time
    // http://localhost/hourly?lat=39.032056&lon=-76.873972&start=2018-11-01&end=2018-12-31&output=html
    year2 += 1;
  }

  mrms = options.includes('mrms') && !explain && !daily && year2 > 2014 && /hourly/.test(req.url) && !stats && !rect;

  getColumns();

  mrms = mrms && cols.includes('precipitation');

  return query(timeOffset);
}; // runQuery

const routeHourly = async(
  req, reply,
  email, start, end, lat, lon,
  attributes, options, explain, output,
  stats, group,
  limit, offset,
  order, where,
) => {
  start = start.replace(/[TZ]/g, ' ');
  end = end.replace(/[TZ]/g, ' ');

  return runQuery({
    req,
    reply,
    type: 'hourly',
    start,
    end,
    format2: 'yyyy-mm-dd HH:00',
    lat,
    lon,
    options,
    url: 'hourly',
    attributes,
    explain,
    email,
    output,
    stats,
    group,
    where,
    order,
    limit,
    offset,
  });
}; // routeHourly

const routeDaily = (
  req, reply,
  email, start, end, lat, lon, attributes, options, explain, output,
  stats, group,
  limit, offset,
  order, where,
  gddbase, gddmax, gddmin,
) => {
  start = start.replace(/[TZ]/g, ' ');
  end = end.replace(/[TZ]/g, ' ');

  return runQuery({
    req,
    reply,
    type: 'hourly',
    start,
    end,
    format2: 'yyyy-mm-dd',
    lat,
    lon,
    options,
    url: 'daily',
    attributes,
    explain,
    email,
    output,
    group,
    stats,
    where,
    order,
    gddbase,
    gddmax,
    gddmin,
    limit,
    offset,
  });
}; // routeDaily

const routeAverages = (
  req, reply,
  email, start, end, lat, lon, attributes, options, explain, output,
  stats, group,
  limit, offset,
  order, where,
) => {
  start = start.replace(/[TZ]/g, ' ');
  end = end.replace(/[TZ]/g, ' ');

  return runQuery({
    req,
    reply,
    type: 'averages',
    start: `${start}`,
    end: `${end}`,
    format2: 'yyyy-mm-dd HH:00',
    lat,
    lon,
    options,
    url: 'averages',
    attributes,
    explain,
    email,
    output,
    group,
    stats,
    where,
    order,
    limit,
    offset,
  });
}; // routeAverages

export { getLocation, routeHourly, routeDaily, routeAverages };