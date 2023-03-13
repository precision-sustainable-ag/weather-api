const {format} = require('sql-formatter');

process.on('uncaughtException', err => {
  console.error(err);
  process.exit(1);
});

const request = require('request');

const {pool, googleAPIKey} = require('./pools');

const fs = require('fs');
const {default: axios} = require('axios');

// NLDAS-2 longitude and latitude:
const xgrid = n =>  (Math.floor( n * 8) / 8).toFixed(3);
const ygrid = n => -(Math.floor(-n * 8) / 8).toFixed(3);

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
let ip;

const init = (req) => {
  ip = req.ip ||
       (req.headers['x-forwarded-for'] || '').split(',').pop() || 
       req.connection.remoteAddress || 
       req.socket.remoteAddress || 
       req.connection.socket.remoteAddress;

  output = req.query.explain ? 'json' : req.query.output || 'json';

  lats = lons = cols = minLat = maxLat = minLon = maxLon = null;
  location = (req.query.location || '').replace(/[^a-z0-9 ]/ig, '').replace(/\s+/g, ' ').toLowerCase();
  options = (req.query.options || '').toLowerCase().split(',');
  rect = options.includes('rect') && (location || (req.query.lat || '').split(',').length == 2);
} // init

const getLocation = (res, func) => {
  if (+location) {
    location = `zip ${location}`;
  }
  pool.query(
    `select * from weather.addresses where address=$1`,
    [location],
    (err, results) => {
      if (err) {
        res.status(200).send(err);
      } else if (results.rows.length) {
        console.log(`Found ${location}`);
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
        request(
          {
            url: `https://maps.googleapis.com/maps/api/geocode/json?address=${location}&key=${googleAPIKey}`,
            json: true,
            // headers: {referer: '128.192.142.200'}
          },
          (err, resp, body) => {
            console.timeEnd(`Looking up ${location}`);
            if (err) {
              res.status(200).send(err);
            } else try {
              let latlon = [];
              let lat;
              let lon;
              let lat1 = body.results[0].geometry.viewport.northeast.lat;
              let lon1 = body.results[0].geometry.viewport.northeast.lng;
              let lat2 = body.results[0].geometry.viewport.southwest.lat;
              let lon2 = body.results[0].geometry.viewport.southwest.lng;

              for (let i in body.results[0].geometry.location) { // why can't I simply do body.results[0].geometry.location.lat ???
                latlon.push(body.results[0].geometry.location[i]);
              }
              [lat, lon] = latlon;

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
            } catch(ee) {
              console.error(ee.message);
            }
          }
        );
      }
    }
  );
} // getLocation

const f = (s) => {
  const ind = ' '.repeat(s.search(/[^\s]/) - 1);
  const rep = s.replace(new RegExp(ind, 'g'), '');

  return rep;
} // f

const sendQuery = (req, res, sq) => {
  let formatted;

  try {
    formatted = format(sq.replace(/::\w+/g, ''), {
      language: 'postgresql',
    });
  } catch(ee) {
    formatted = sq;
  }

  const process = (rows) => {
    if (!rows.length) {
      res.send('No data found');
      return;
    }

    // prevent duplicate rows.  screws up LIMIT unfortunately
    let lastJSON;
    rows = rows.filter(row => lastJSON != (lastJSON = JSON.stringify(row)));

    switch (output ? output.toLowerCase() : 'json') {
      case 'csv':
        s = Object.keys(rows[0]).toString() + '\n' +
            rows.map(r => Object.keys(r).map((v, i, a) => r[v])).join('\n');
            // rows.map(r => Object.values(r).toString()).join('<br>');

        res.set('Content-Type', 'text/csv');
        res.setHeader(`Content-disposition`, `attachment; filename=${lats}.${lons}.HourlyAverages.csv`);
        res.send(s);
        break;
      
      case 'html':
        s = `<script src="https://aesl.ces.uga.edu/scripts/d3/d3.js"></script>
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
                 <tr>${rows.map(r => '<td>' + Object.keys(r).map((v, i, a) => r[v]).join('<td>')).join('<tr>')}</tr>
               </tbody>
             </table>

             <script src="https://aesl.ces.uga.edu/weatherapp/src/weather.js"></script>
            `;
        res.status(200).send(s);
        break;

      default:
        if (req.query.explain) {
          res.status(200).json({
            query: sq.slice(8).trim(),
            rows : rows
          });
        } else {
          if (req.callback) {
            req.callback(rows);
          } else {
            res.status(200).json(rows);
          }
        }
    }
  } // process

  let qq = sq.replace(/'/g, '');

  pool.query('delete from weather.queries where date < now() - interval \'30 day\'');

  pool.query(`select results from weather.queries where query='${qq}'`, (err, results) => {
    if (err) {
      console.error(err);
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
      let startTime = new Date();

      if (req.query.explain) {
        sq = 'explain ' + sq;
      }

      pool.query(sq, (err, results) => {
        if (err) {
          console.error(sq);
          console.error(err);
          console.error('_'.repeat(80));
          res.status(500).send(err);
          return;
        }

        if (
          !req.query.explain &&
          (
            /averages?/.test(req.originalUrl) ||
            (req.query.end && new Date() - new Date(req.query.end) > 86400000)
          )
        ) {
          let jr = JSON.stringify(results.rows);
          let q = `insert into weather.queries (date, url, query, results) values (now(), '${req.originalUrl}', '${qq}', '${jr}')`;

          pool.query(q, (err, results) => {
            if (err) {
              console.error('queries_error', err);
            }
          });
        }

        if (!req.query.nosave) {
          let time = new Date() - startTime;
          let hits = `insert into weather.hits
                      (date, ip, query, ms)
                      values (now(), '${ip}', '${req.url}', ${time})
                     `;
          pool.query(hits);
        }
        process(results.rows);
      });
    }
  });
} // sendQuery

const mround = n => (Math.round((n - 0.005) * 100) / 100 + 0.005).toFixed(3);

const runQuery = (req, res, type, start, end, format, daily) => {
  const query = (offset) => {
    let byx;
    let byy;
    let rtables = {};
    let latlons = [];

    if (rect) {
      byy = Math.max(0.125, 0.125 * (maxLat - minLat | 0));
      byx = Math.max(0.125, 0.125 * (maxLon - minLon | 0));

      for (let y = minLat; y <= maxLat; y += byy) {
        for (let x = minLon; x <= maxLon; x += byx) {
          rtables[`weather.${type}${Math.trunc(ygrid(y))}_${-Math.trunc(xgrid(x))}`] = true;
          latlons.push(`'${+ygrid(y)}${+xgrid(x)}'`);
        }
      }
      rtables = Object.keys(rtables);
    }

    let sq;
    let cond = where ? ` and (${where})` : '';
    let dateCond = `date::timestamp + interval '${offset} seconds' between '${start}'::timestamp and '${end}'::timestamp`;
    let tables = rect ? rtables
                          .map((table, i) =>
                            f(`
                               select lat as rlat, lon as rlon, *
                               from (
                                 ${years.map(
                                     year => type == 'ha_' ? `select * from ${table}` : `select * from ${table}_${year}`
                                   ).join(' union all ')
                                 }
                               ) a
                               where lat::text || lon in (${latlons}) and
                                     date::timestamp + interval '${offset} seconds' between '${start}' and '${end}'
                                     ${cond}
                              `
                            )
                          )
                          .join(' union all\n')
                      : lats
                          .map((lat, i) => {
                            let mainTable = type == 'nldas_hourly_' ?
                                               years.map(
                                                 year => {
                                                    return f(`
                                                      select *, precipitation as nldas
                                                      from weather.${type}${Math.trunc(ygrid(lat))}_${-Math.trunc(xgrid(lons[i]))}_${year}
                                                      where lat=${ygrid(lat)} and lon=${xgrid(lons[i])} and
                                                            ${dateCond}
                                                  `);
                                                }
                                               ).join(' union all ')
                                             : f(`
                                                  select *
                                                  from weather.${type}${Math.trunc(ygrid(lat))}_${-Math.trunc(xgrid(lons[i]))}
                                                  where lat=${ygrid(lat)} and lon=${xgrid(lons[i])} and
                                                        ${dateCond}
                                                 `
                                               );

                            if (type == 'nldas_hourly_' && (req.query.predicted == 'true' || options.includes('predicted'))) {
                              let dc = `date::timestamp + interval '${offset} seconds' between '${start}'::timestamp and '${end}'::timestamp`;
                              let years = [];

                              for (let i = +start.slice(0, 4); i <= +end.slice(0, 4) + 1; i++) {
                                years.push(i);
                              }

                              dc = `date::timestamp + interval '${offset} seconds' > '2099-10-10'`;

                              let maxdate = '';
                              const year = new Date().getFullYear();

                              if (mainTable) {
                                mainTable += ' union all ';
                                maxdate = ` date > (
                                              select max(date) from (
                                                select date from weather.${type}${Math.trunc(ygrid(lat))}_${-Math.trunc(xgrid(lons[i]))}_new union all
                                                select date from weather.${type}${Math.trunc(ygrid(lat))}_${-Math.trunc(xgrid(lons[i]))}_${year}
                                              ) a
                                            )
                                            and
                                          `;
                              }

                              mainTable +=
                                years
                                  .filter(year => year != 'new')
                                  .map(year => `
                                    select * from (
                                      select
                                        make_timestamp(${year}, extract(month from date)::integer, extract(day from date)::integer, extract(hour from date)::integer, 0, 0) as date,
                                        lat, lon, air_temperature, humidity, pressure, zonal_wind_speed, meridional_wind_speed, longwave_radiation, convective_precipitation, potential_energy, potential_evaporation, precipitation, shortwave_radiation, null::boolean as frost, relative_humidity, wind_speed, precipitation as nldas
                                        ,
                                        null::real as evp,
                                        null::real as weasd,
                                        null::real as snod,
                                        null::real as albdo,
                                        null::real as tsoil,
                                        null::real as veg,
                                        null::real as snom,
                                        null::real as nswrs,
                                        null::real as nlwrs,
                                        null::real as lhtfl,
                                        null::real as shtfl,
                                        null::real as avsft,
                                        null::real as gflux,
                                        null::real as asnow,
                                        null::real as arain,
                                        null::real as acond,
                                        null::real as ccond,
                                        null::real as lai,
                                        null::real as sbsno,
                                        null::real as evbs,
                                        null::real as evcw,
                                        null::real as dswrf,
                                        null::real as dlwrf,
                                        null::real as trans,
                                        null::real as cnwat,
                                        null::real as snohf,
                                        null::real as bgrun,
                                        null::real as ssrun,
                                        null::real as snowc,
                                        null::real as soilm1,
                                        null::real as soilm2,
                                        null::real as soilm3,
                                        null::real as soilm4,
                                        null::real as soilm5,
                                        null::real as soilm6,
                                        null::real as mstav1,
                                        null::real as mstav2
                                      from weather.ha_${Math.trunc(ygrid(lat))}_${-Math.trunc(xgrid(lons[i]))}
                                    ) a
                                    where lat=${ygrid(lat)} and lon=${xgrid(lons[i])} and
                                          ${maxdate}
                                          ${dateCond}
                                  `
                                ).join(' union all ');
                              
                              // res.status(200).send(mainTable);
                            }
                            
                            if (mrms && years.length) {
                              let mrmsTable;

                              mrmsTable = `
                                (${years.map(year => `
                                    select * from weather.mrms_${Math.trunc(mround(lat))}_${-Math.trunc(mround(lons[i]))}_${year}
                                    where lat = ${mround(lat)} and lon = ${mround(lons[i])} and ${dateCond}
                                  `).join(' union all ')
                                  }
  
                                  union all
  
                                  select * from weather.mrms_${Math.trunc(ygrid(lat))}_${-Math.trunc(xgrid(lons[i]))}_new
                                  where lat = ${mround(lat)} and lon = ${mround(lons[i])} and ${dateCond}
  
                                  union all
  
                                  select b.date, lat, lon, precipitation from weather.mrmsmissing a
                                  left join (${mainTable}) b
                                  on a.date = b.date
                                  where lat=${ygrid(lat)} and lon=${xgrid(lons[i])}
                                ) m
                              `;

                              // originally select distinct:
                              return `
                                select ${lat} as rlat, ${lons[i]} as rlon, *
                                from (
                                  select coalesce(a.date, b.date) as date,
                                         coalesce(a.lat, b.lat) as lat,
                                         coalesce(a.lon, b.lon) as lon,
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
                                         coalesce(b.precipitation, 0) as precipitation,
                                         nldas,
                                         frost
                                  from (
                                    ${mainTable}
                                  ) a
                                  full join (
                                    select * from ${mrmsTable}
                                  ) b
                                  on a.date = b.date
                                ) alias1
                                where ${dateCond}
                                      ${cond}
                              `;
                            } else if (mpe) {
                              let mpeTable = `weather.mpe_hourly_${Math.trunc(ygrid(lat))}_${-Math.trunc(xgrid(lons[i]))}`;

                              return `select ${lat} as rlat, ${lons[i]} as rlon, *
                                      from (
                                        select a.*, mpe
                                        from weather.${type}${Math.trunc(ygrid(lat))}_${-Math.trunc(xgrid(lons[i]))} a
                                        left join (
                                          select * from ${mpeTable}
                                          where (lat, lon) in (
                                            select lat, lon
                                            from ${mpeTable}
                                            where abs(lat - ${lat}) < 0.125 and abs(lon - ${lons[i]}) < 0.125
                                            order by sqrt(power(lat - ${lat}, 2) + power(lon - ${lons[i]}, 2))
                                            limit 1
                                          )                                 
                                        ) b
                                        on a.date = b.date
                                      ) alias1
                                      where lat=${ygrid(lat)} and lon=${xgrid(lons[i])} and
                                            date::timestamp + interval '${offset} seconds' between '${start}' and '${end}'
                                            ${cond}
                                     `;
                            } else {
                              return `select ${lat} as rlat, ${lons[i]} as rlon, *
                                      from (${mainTable}) a
                                      where lat=${ygrid(lat)} and lon=${xgrid(lons[i])} and
                                            date::timestamp + interval '${offset} seconds' between '${start}' and '${end}'
                                            ${cond}
                                     `;
                            }
                          })
                          .join(' union all\n');

    // if (req.query.predicted == 'true') {
    //    res.status(200).send(tables.replace(/[\n\r]+/g, '<br>')); return;
    // }

    let order = req.query.order || `1 ${cols.split(/\s*,\s*/).includes('lat') ? ',lat' : ''} ${cols.split(/\s*,\s*/).includes('lon') ? ',lon' : ''}`;

    if (daily) {
      sq = `select to_char(date::timestamp + interval '${offset} seconds', '${format}') as date,
                   ${cols.replace(/\blat\b/, 'rlat as lat').replace(/\blon\b/, 'rlon as lon')}
            from (
              select date as GMT, *
              from (${tables}) tables
            ) a
            group by to_char(date::timestamp + interval '${offset} seconds', '${format}'), rlat, rlon
            order by ${order}
           `;
    } else {
      let other = '';
      let zgy = `replace(extract(year from (date::timestamp + interval '${offset} seconds' - interval '5 months'), '5', '6') as growingyear, `;
      let gy =  `(extract(year from (date::timestamp + interval '${offset} seconds' - interval '5 months')))::text || '-' ||
                 (extract(year from (date::timestamp + interval '${offset} seconds' - interval '5 months')) + 1)::text
                 as growingyear, `;
    
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
                   .replace(/\bdoy\b/g,   `extract(doy from date::timestamp + interval '${offset} seconds') as doy, `)
                   .replace(/\bmonth\b/g, `extract(month from date::timestamp + interval '${offset} seconds') as month, `)
                   .replace(/\byear\b/g,  `extract(year from date::timestamp + interval '${offset} seconds') as year, `)
                   .replace(/\bgrowingyear\b/g, gy);
      }

      sq = `select ${other} to_char(date::timestamp + interval '${offset} seconds', '${format}') as date,
                   ${cols.replace(/\blat\b/, 'rlat as lat').replace(/\blon\b/, 'rlon as lon')}
            from (
              select date as GMT, *
              from (${tables}) tables
            ) a
            order by ${order}
           `;
    }

    if (stats) {
      sq = `select ${group ? group + ', ' : ''} ${stats}
            from (
              ${sq}
            ) alias
            ${group ? 'group by ' + group : ''}
           `;
    }

    if (req.query.gaws) {
      attr = (req.query.attr || '').split(',');
      sq = f(`
             select *
             from (
               ${sq}
             ) a
             left join (
               select ${attr.includes('air_temperature') ?
                          `min_air_temperature as ws_min_air_temperature,
                           max_air_temperature as ws_max_air_temperature,
                           avg_air_temperature as ws_avg_air_temperature,` :
                        ''
                      }
                      ${attr.includes('soil_temperature') ?
                          `min_soil_temperature_10cm as ws_min_soil_temperature,
                           max_soil_temperature_10cm as ws_max_soil_temperature,
                           avg_soil_temperature_10cm as ws_avg_soil_temperature,` :
                        ''
                      }
                      ${attr.includes('soil_temperature') ?
                          `min_water_temp as ws_min_water_temperature,
                           max_water_temp as ws_max_water_temperature,
                           avg_water_temp as ws_avg_water_temperature,` :
                        ''
                      }
                      ${attr.includes('pressure') ?
                          `min_atmospheric_pressure as ws_min_pressure,
                           max_atmospheric_pressure as ws_max_pressure,
                           avg_atmospheric_pressure as ws_avg_pressure,` :
                        ''
                      }
                      ${attr.includes('relative_humidity') ?
                          `min_humidity / 100 as ws_min_relative_humidity,
                           max_humidity / 100 as ws_max_relative_humidity,
                           avg_humidity / 100 as ws_avg_relative_humidity,` :
                        ''
                      }
                      ${attr.includes('dewpoint') ?
                          `min_dewpoint as ws_min_dewpoint,
                           max_dewpoint as ws_max_dewpoint,
                           avg_dewpoint as ws_avg_dewpoint,` :
                        ''
                      }
                      ${attr.includes('vapor_pressure') ?
                          `min_vapor_pressure as ws_min_vapor_pressure,
                           max_vapor_pressure as ws_max_vapor_pressure,
                           avg_vapor_pressure as ws_avg_vapor_pressure,` :
                        ''
                      }
                      date
               from weather.stationdata
               where site=${req.query.gaws}
             ) b
             on a.date::date = b.date::date
           `);
    }

    if (req.query.limit) {
      sq = `select * from (${sq}) a limit ${req.query.limit} offset ${req.query.offset}`;
    }

    // console.error(sq);
    sendQuery(req, res, sq);
  } // query

  const lookup = () => {
    if (options.includes('gmt') || options.includes('utc')) {
      return query(0);
    } else {
      pool.query(
        `select * from weather.timezone
         where lat=${ygrid(lats[0])} and lon=${xgrid(lons[0])}
        `,
        (err, results) => {
          try {
            if (err) {
              res.status(200).send(err);
            } else if (results.rows.length) {
              return query(results.rows[0].rawoffset);
            } else {
              console.time('Getting timezone');
              request(
                {
                  url: `https://maps.googleapis.com/maps/api/timezone/json?location=${ygrid(lats[0])},${xgrid(lons[0])}&timestamp=0&key=${googleAPIKey}`,
                  json: true
                },
                (err, resp, body) => {
                  console.timeEnd('Getting timezone');
                  try {
                    pool.query(`insert into weather.timezone
                                (lat, lon, dstOffset, rawOffset, timeZoneId, timeZoneName)
                                values (${ygrid(lats[0])}, ${xgrid(lons[0])}, ${body.dstOffset}, ${body.rawOffset}, '${body.timeZoneId}', '${body.timeZoneName}')
                               `
                              );
                    return query(body.rawOffset);
                  } catch (ee) {
                    res.status(200).send('error');
                  }
                }
              );
            }
          } catch(ee) {
            console.error('lookup error: ' + ee.message);
          }
        }
      );
    }
  } // lookup

  const clean = (s) => {
    let t = decodeURI(s).replace(/["()+\-*/<>,= 0-9\.]|doy|day|month|year|growingyear|sum|min|max|avg|count|stddev_pop|stddev_samp|variance|var_pop|var_samp|date|as|abs|and|or|not|between|tmp|air_temperature|spfh|humidity|relative_humidity|pres|pressure|ugrd|zonal_wind_speed|wind_speed|vgrd|meridional_wind_speed|dlwrf|longwave_radiation|frain|convective_precipitation|cape|potential_energy|pevap|potential_evaporation|apcp|precipitation|mrms|dswrf|shortwave_radiation|frost|gdd/ig, '');

    if (t) {
      console.error('*'.repeat(80));
      console.error(t);
      console.error('*'.repeat(80));
      return 'ERROR';
    } else {
      return s;
    }
  } // clean

  const fix = (col, alias) => {
    return col.replace(/\btmp\b/i,   'air_temperature'           + (alias ? ' as TMP' : ''))
              .replace(/\bspfh\b/i,  'humidity'                  + (alias ? ' as SPFH' : ''))
              .replace(/\bpres\b/i,  'pressure'                  + (alias ? ' as PRES' : ''))
              .replace(/\bugrd\b/i,  'zonal_wind_speed'          + (alias ? ' as UGRD' : ''))
              .replace(/\bvgrd\b/i,  'meridional_wind_speed'     + (alias ? ' as VGRD' : ''))
              .replace(/\bdlwrf\b/i, 'longwave_radiation'        + (alias ? ' as DLWRF' : ''))
              .replace(/\bfrain\b/i, 'convective_precipitation'  + (alias ? ' as FRAIN' : ''))
              .replace(/\bcape\b/i,  'potential_energy'          + (alias ? ' as CAPE' : ''))
              .replace(/\bpevap\b/i, 'potential_evaporation'     + (alias ? ' as PEVAP' : ''))
              .replace(/\bapcp\b/i,  'precipitation'             + (alias ? ' as APCP' : ''))
              .replace(/\bdswrf\b/i, 'shortwave_radiation'       + (alias ? ' as DSWRF' : ''))
  } // fix

  const getColumns = () => {
    if (daily) {
      if (attr) {
        cols = attr.toLowerCase()
                   .replace(/,?gdd/g, '') // included automatically if req.query.gddbase 
                   .split(',')
                   .map(col =>
                          /^(lat|lon)$/.test(col) ? col :
                          /mrms|precipitation|radiation|potential/.test(col) ?
                            `sum(${fix(col, false)}) as ${col}` :
                            `min(${fix(col, false)}) as min_${col}, max(${fix(col, false)}) as max_${col}, avg(${fix(col, false)}) as avg_${col}`
                       )
                   .join(',');
      } else {
        cols = `lat, lon,
                sum(precipitation)            as precipitation,
                sum(longwave_radiation)       as longwave_radiation,
                sum(shortwave_radiation)      as shortwave_radiation,
                sum(potential_energy)         as potential_energy,
                sum(potential_evaporation)    as potential_evaporation,
                sum(convective_precipitation) as convective_precipitation,
                min(air_temperature)          as min_air_temperature,          max(air_temperature)          as max_air_temperature,          avg(air_temperature)          as avg_air_temperature,
                min(humidity)                 as min_humidity,                 max(humidity)                 as max_humidity,                 avg(humidity)                 as avg_humidity,
                min(relative_humidity)        as min_relative_humidity,        max(relative_humidity)        as max_relative_humidity,        avg(relative_humidity)        as avg_relative_humidity,
                min(pressure)                 as min_pressure,                 max(pressure)                 as max_pressure,                 avg(pressure)                 as avg_pressure,
                min(zonal_wind_speed)         as min_zonal_wind_speed,         max(zonal_wind_speed)         as max_zonal_wind_speed,         avg(zonal_wind_speed)         as avg_zonal_wind_speed,
                min(meridional_wind_speed)    as min_meridional_wind_speed,    max(meridional_wind_speed)    as max_meridional_wind_speed,    avg(meridional_wind_speed)    as avg_meridional_wind_speed,
                min(wind_speed)               as min_wind_speed,               max(wind_speed)               as max_wind_speed,               avg(wind_speed)               as avg_wind_speed
               `;
      }
      let {gddbase} = req.query;
      if (gddbase) {
        const mintemp = req.query.gddmin || gddbase;
        const maxtemp = req.query.gddmax || 999;
      
        cols += `,
          greatest(0, (least(${maxtemp}, max(air_temperature)) + greatest(${mintemp}, least(${maxtemp}, min(air_temperature)))) / 2 - ${gddbase}) as gdd
        `;
      }
    } else {
      cols = attr ? fix(attr, true)
                  : 'lat, lon, air_temperature, humidity, relative_humidity, pressure, zonal_wind_speed, meridional_wind_speed, wind_speed, longwave_radiation, convective_precipitation, potential_energy, potential_evaporation, precipitation, shortwave_radiation, frost';

      if (/averages|daily/.test(req.url)) {
        cols = cols.replace(', frost', '');
      }
    }

  } // getColumns

  let attr = (req.query.attributes || req.query.attr || '').replace(/(soil_temperature|water_temperature|dewpoint|vapor_pressure),?/g, '').replace(/,$/, '');
  let mpe  = /\bmpe\b/i.test(attr);
  let mrms = /hourly|daily/.test(req.url) && !/nomrms/.test(options);
  let year1 = Math.max(+start.slice(0, 4), 2015);
  let year2 = Math.min(+end.slice(0, 4), new Date().getFullYear());
  let years = [];
  let group = req.query.group;
  let where = req.query.where ?
                clean(fix(req.query.where))
                  .replace(/month/g, 'extract(month from date)') :
                '';
  let stats = req.query.stats ?
                clean(fix(req.query.stats.replace(/[^,]+/g, s => `${s} as "${s}"`))) :
                '';

  if (attr && /averages|daily/.test(req.url)) {
    attr = attr.replace(/, *frost/, '').replace(/, *nldas/, '');
  }

  for (let i = year1; i <= Math.min(year2 + 1, new Date().getFullYear()); i++) {
    years.push(i);
  }

  if (year2 == new Date().getFullYear()) {
    years.push('new');
  }

  getColumns();

  if (location) {
    getLocation(res, lookup);
  } else {
    lats = (req.query.lat || '').split(',');
    lons = (req.query.lon || '').split(',');
    if (rect && lats.length == 2) {
      minLat = Math.min(...lats);
      maxLat = Math.max(...lats);
      minLon = Math.min(...lons);
      maxLon = Math.max(...lons);
    }
    lookup();
  }
} // runQuery

const getHourly = (req, res) => {
  const start = req.query.start || '2000-01-01';
  const end   = req.query.end ? req.query.end + (/:/.test(req.query.end) ? '' : ' 23:59') :
                                '2099-12-31 23:59'; 

  init(req);
  runQuery(req, res, 'nldas_hourly_', start, end, 'YYYY-MM-DD HH24:MI');
} // getHourly

const getDaily = (req, res) => {
  const start = req.query.start || '2000-01-01';
  const end   = req.query.end ? req.query.end + ' 23:59' : '2099-12-31 23:59';

  init(req);

  runQuery(req, res, 'nldas_hourly_', start, end, 'YYYY-MM-DD', true);
} // getDaily

const getAverages = (req, res) => {
  let start = req.query.start || '01-01';
  let end   = req.query.end ? req.query.end + ' 23:59' : '12-31 23:59';

  init(req);
  if (start.split('-').length == 3) { // drop year
    start = start.slice(start.indexOf('-') + 1);
  }

  if (end.split('-').length == 3) { // drop year
    end = end.slice(end.indexOf('-') + 1);
  }

  runQuery(req, res, 'ha_', '2099-' + start, '2099-' + end, 'MM-DD HH24:MI');
} // getAverages

const queryJSON = (req, res, sq) => {
  pool.query(
    `${sq}
     limit ${req.query.limit || 100000}
     offset ${req.query.offset || 0}
    `,
    (err, results) => {
      if (err) {
        console.error(err);
        res.status(500).send(err);
      } else {
        res.status(200).json(results.rows);
      }
    }
  );
} // queryJSON

const GAWeatherStations = (req, res) => {
  queryJSON(req, res, `select * from weather.stations order by County`);
} // GAWeatherStations

const addresses = (req, res) => {
  queryJSON(req, res, `select * from weather.addresses order by address`);
} // addresses

const indexes = (req, res) => {
  queryJSON(req, res, `select * from pg_indexes where tablename not like 'pg%' order by indexname`);
} // indexes

const tables = (req, res) => {
  queryJSON(req, res, `
    select table_name as table, reltuples as rows from (
      select * from information_schema.tables
      where table_schema='weather'
    ) a
    left join pg_class b
    on a.table_name = b.relname
    where reltuples > 0
    order by table_name
  `);
} // tables

const counttablesrows = (req, res) => {
  queryJSON(req, res, `
    select
      count(*) as tables,
      sum(reltuples) as rows
    from (
      select * from information_schema.tables
      where table_schema='weather'
    ) a
    left join pg_class b
    on a.table_name = b.relname
    where reltuples > 0    
  `);
} // counttablesrows

const countindexes = (req, res) => {
  queryJSON(req, res, `
    select count(*) as indexes
    from pg_indexes
    where schemaname = 'weather'
  `);
} // countindexes

const databasesize = (req, res) => {
  queryJSON(req, res, `
    select pg_size_pretty(pg_database_size('postgres')) as size
  `);
} // databasesize

const hits = (req, res) => {
  queryJSON(
    req,
    res,
    `
     select * from weather.hits
     where query not like '%explain%' and query not like '%nvm%' and
           (date > current_date - 1 or (ip <> '::ffff:172.18.186.142' and query not like '%25172.18.186%25'))
     order by date desc
    `
  );
} // hits

const mvm = (req, res) => {
  const sq = `
    select a.sum - b.sum as delta,
           a.lat as alat, a.lon as alon, a.sum as asum,
           b.lat as blat, b.lon as blon, b.sum as bsum
    from weather.mrms_${req.query.lat}_${-req.query.lon}_2019_annual a
    left join weather.mrms_${req.query.lat}_${-req.query.lon}_2019_annual b
    on (a.lat between b.lat + 0.01 and b.lat + 0.011 and
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
      res.status(200).send('ERROR:<br>' + sq.replace(/\n/g, '<br>').replace(/ /g, '&nbsp;'));
    } else {
      res.status(200).send(JSON.stringify(results.rows));
    }
  });    
} // mvm

const nvm = (req, res) => {
  const NVMprocess = () => {
    let sq1 = `select b.year as "Year", coalesce(round(a.total), 0) as "MRMS<br>precipitation", round(b.totalz) as "NLDAS<br>precipitation" from (
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
              on a.year = b.year
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
              on a.month = b.month
              order by 1
             `;

    if (req.query.explain) {
      sql = 'explain ' + sql;
    }

    pool.query(sq1, (err, results) => {
      const data = results => 
        `<table id="Data">
           <thead>
             <tr><th>${Object.keys(results.rows[0]).join('<th>')}<th>RPD</tr>
           </thead>
           <tbody>
             <tr>${
                   results.rows.map(r => {
                     let m = r['MRMS<br>precipitation'];
                     let n = r['NLDAS<br>precipitation'];
                     let rpd = Math.round(Math.abs(m -n ) / ((m + n) / 2) * 100) || 0;
                     let style = Math.abs(m - n) > 50.8 && rpd > 50 ? 'background: red; color: white; font-weight: bold;' :
                                 Math.abs(m - n) > 50.8 && rpd > 35 ? 'background: orange' :
                                 Math.abs(m - n) > 50.8 && rpd > 20 ? 'background: yellow' :
                                                          '';

                     return '<td>' + 
                            Object.keys(r).map(v => r[v]).join('<td>') +
                            '<td style="' + style + '">' + rpd;
                   }).join('<tr>')
                 }
             </tr>
           </tbody>
         </table>
        `;

      let s = `<link rel="stylesheet" href="/css/weather.css">
               <style>th {width: 10em;}</style>
              `;

      if (err) {
        res.status(200).send('ERROR:<br>' + sq1.replace(/\n/g, '<br>').replace(/ /g, '&nbsp;'));
        return;
      } else {
        s += data(results) + '<hr>';
      }

      if (req.query.explain) {
        sq2 = 'explain ' + sq2;
      }

      pool.query(sq2, (err, results) => {
        if (err) {
          res.status(200).send('ERROR:<br>' + sq2.replace(/\n/g, '<br>').replace(/ /g, '&nbsp;'));
        } else {
          s += data(results);
          res.status(200).send(s);
        }
      });    
    });
  } // NVMprocess

  let mlat;
  let mlon;
  let nlat;
  let nlon;

  init(req);

  if (location) {
    getLocation(res, (lats, lons) => {
      mlat = mround(lats);
      mlon = mround(lons);
      nlat = ygrid(lats);
      nlon = xgrid(lons);

      NVMprocess();
    });
  } else {
    mlat = mround(req.query.lat);
    mlon = mround(req.query.lon);
    nlat = ygrid(req.query.lat);
    nlon = xgrid(req.query.lon);

    NVMprocess();
  }
} // nvm

const nvm2 = (req, res) => {
  try {
    const runQuery = () => {
      let sq = `select n.date, n.lat, n.lon, nldas, coalesce(mrms, 0) as mrms from (
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
            res.status(200).send(err);
          } else {
            try {
              s = `<link rel="stylesheet" href="css/weather.css">
                   <link rel="stylesheet" href="//aesl.ces.uga.edu/weatherapp/src/nvm2.css">
                   <script src="https://aesl.ces.uga.edu/scripts/jquery/jquery.js"></script>
                   <script src="https://aesl.ces.uga.edu/scripts/jqLibrary.js"></script>
                   <script src="https://aesl.ces.uga.edu/weatherapp/src/nvm2.js"></script>
                   <script>let monthly = ${JSON.stringify(results.rows)};</script>
                   <div id="Data"></div>
                  `;

              pool.query(
                `select to_char(date, 'yyyy-mm-dd HH:00') as "Date", lat as "Lat", lon as "Lon",
                        round(nldas) as "NLDAS",
                        round(mrms)  as "MRMS",
                        round(mrms - nldas) as "&Delta;"
                 from (
                   ${sq}
                 ) alias
                 where abs(mrms - nldas) > 13
                `,
                (err, results) => {
                  if (err || !results) {
                    res.status(200).send(err);
                  } else try {
                    if (results.rowCount) {
                      s +=  `<hr>
                             <table id="Flags">
                               <thead>
                                 <tr><th>${Object.keys(results.rows[0]).join('<th>')}
                               </thead>
                               <tbody>
                                 <tr>${
                                       results.rows.map(r =>
                                         Object.keys(r).map(v => `<td>${r[v]}`).join('')
                                       ).join('<tr>')
                                     }
                                 </tr>
                               </tbody>
                             </table>
                             <hr>
                            `;
                    }

                    pool.query(
                      `insert into weather.nvm2 (lat, lon, year, data)
                       values (${lat}, ${lon}, ${year}, '${s.replace(/ /g, ' ').trim()}')
                      `
                    );

                    res.status(200).send(s);
                  } catch(e) {
                    res.status(200).send(e.message);
                  }
                }
              );
            } catch(ee) {
              res.status(200).send(ee.message);      
            }
          }
        }
      );
    } // runQuery

    let lat = Math.round(req.query.lat);
    let lon = Math.round(req.query.lon);
    let year = req.query.year;
    let s;

    init(req);

    pool.query(
      `select data from weather.nvm2 where lat = ${lat} and lon = ${lon} and year = ${year}`,
      (err, results) => {
        if (err) {
          res.status(200).send(err);
        } else if (results.rowCount) {
          res.status(200).send(results.rows[0].data);
        } else {
          runQuery();
        }
      }
    );
  } catch(ee) {
    res.status(200).send(ee.message);
  }
} // nvm2

const nvm2Data = (req, res) => {
  init(req);

  pool.query(
    `select distinct lat, lon, year from weather.nvm2`,
    (err, results) => {
      res.status(200).send(JSON.stringify(results.rows));
    }
  );
} // nvm2Data

const nvm2Update = (req, res) => {
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
  res.status(200).send(sq);
} // nvm2Update

const nvm2Query = (req, res) => {
  try {
    let sq = `select lat, lon from weather.nvm2
              where ${req.query.condition.replace(/select|insert|update|drop|delete/ig, '')}
              order by lat, lon
             `;

    pool.query(
      sq,
      (err, results) => {
        if (err) {
          res.status(200).send(err);
        } else if (results.rowCount) {
          res.status(200).send(JSON.stringify(results.rows));
        }        
      }
    );
  } catch(ee) {
    console.error(ee.message);
  }
} // nvm2Query

const isMissing = (res, parms) => {
  const error = [];

  Object.keys(parms).forEach(key => {
    if (!parms[key]) {
      error.push(key);
    }
  });

  if (error.length) {
    res.status(400).send({ERROR: `Missing ${error}`});
    return true;
  } else {
    return false;
  }
} // isMissing

const rosetta = (req, res) => {
  axios.post(
    'https://www.handbook60.org/api/v1/rosetta/1',
    {
      soildata: req.body.soildata
    }
  ).then(data => {
    res.send(data.data);
  });
} // rosetta

const watershed = (req, res) => {
  const query = (sq) => {
    console.log(sq);
    pool.query(
      sq,
      (err, results) => {
        if (err) {
          res.status(500).send(err);
        } else if (results.rows.length) {
          res.send(results.rows.map((row) => {
            delete row.geometry;
            return row;
          }));
        } else {
          res.send({});
        }
      }
    );
  } // query

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
      `
    );
  } // latLon

  init(req);
  
  let attributes =
    req.query.attributes?.split(',')
      .map(attr => (
        attr
          .trim()
          .replace(/^name$/, 'huc12.name')
          .replace(/huc(\d+)name/, (_, s) => `huc${s}.name as huc${s}name`)
          .replace(/polygon/i, 'ST_AsText(geometry) as polygon')
      ));
  
  const polygon = req.query.polygon;

  const state = req.query.state;
  
  if (!attributes) {
    if (polygon === 'true') {
      attributes = `huc12, huc12.name, huc10, huc10.name as huc10name, huc8, huc8.name as huc8name, huc6, huc6.name as huc6name, huc4, huc4.name as huc4name, huc2, huc2.name as huc2name,tnmid,metasourceid,sourcedatadesc,sourceoriginator,sourcefeatureid,loaddate,referencegnis_ids,areaacres,areasqkm,states,hutype,humod,tohuc,noncontributingareaacres,noncontributingareasqkm,globalid,shape_Length,shape_Area, (ST_AsGeoJSON(geometry)::jsonb->'coordinates') as polygonarray, ST_AsText(geometry) as polygon`;
    } else {
      attributes = 'huc12, huc12.name, huc10, huc10.name as huc10name, huc8, huc8.name as huc8name, huc6, huc6.name as huc6name, huc4, huc4.name as huc4name, huc2, huc2.name as huc2name,tnmid,metasourceid,sourcedatadesc,sourceoriginator,sourcefeatureid,loaddate,referencegnis_ids,areaacres,areasqkm,states,hutype,humod,tohuc,noncontributingareaacres,noncontributingareasqkm,globalid,shape_Length,shape_Area';
    }
  }

  const huc = req.query.huc;

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
      `
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
      `
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
} // watershed

const mlra = (req, res) => {
  const query = (sq) => {
    console.log(sq);
    pool.query(
      sq,
      (err, results) => {
        if (err) {
          res.status(500).send(err);
        } else if (results.rows.length) {
          res.send(results.rows.map((row) => {
            delete row.geometry;
            return row;
          }));
        } else {
          res.send({});
        }
      }
    );
  } // query

  const latLon = () => {
    if (mlra) {
      query(`
        SELECT
          b.id,b.name,b.mlrarsym,b.lrrsym,b.lrrname,
          string_agg(DISTINCT county || ' County' || ' ' || state, ', ') as counties,
          string_agg(DISTINCT state, ', ') as states,
          string_agg(DISTINCT state_code,', ') as state_codes,
          string_agg(DISTINCT countyfips, ', ') as countyfips,
          string_agg(DISTINCT statefips, ', ') as statefips
          ${polygon ? ', polygon' : ''}
        FROM counties a
        RIGHT JOIN (
          SELECT *, ST_AsText(geometry) as polygon
          FROM mlra.mlra
          WHERE mlrarsym = '${mlra}'
        ) b
        ON ST_Intersects(ST_SetSRID(b.geometry, 4269), a.geometry)
        GROUP BY b.id,b.name,b.mlrarsym,b.lrrsym,b.lrrname ${polygon ? ', polygon' : ''}
      `);
    } else {
      query(
        `
          SELECT distinct ${attributes}
          FROM mlra.mlra
          WHERE ST_Contains(geometry, ST_GeomFromText('POINT(${lons[0]} ${lats[0]})'))
        `
      );
    }
  } // latLon

  init(req);
  
  let attributes =
    req.query.attributes?.split(',')
      .map(attr => (
        attr
          .trim()
          .replace(/polygon/i, 'ST_AsText(geometry) as polygon')
      ));
  
  const polygon = req.query.polygon;
  const mlra = req.query.mlra;

  if (!attributes) {
    if (polygon === 'true') {
      attributes = `id,name,mlrarsym,lrrsym,lrrname,(ST_AsGeoJSON(geometry)::jsonb->'coordinates') as polygonarray, ST_AsText(geometry) as polygon`;
    } else {
      attributes = 'id,name,mlrarsym,lrrsym,lrrname';
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
} // mlra

const county = (req, res) => {
  const query = (sq) => {
    console.log(sq);
    pool.query(
      sq,
      (err, results) => {
        if (err) {
          res.status(500).send(err);
        } else if (results.rows.length) {
          res.send(results.rows.map((row) => {
            delete row.geometry;
            return row;
          }));
        } else {
          res.send({});
        }
      }
    );
  } // query

  const latLon = () => {
    query(
      `
        SELECT distinct ${attributes}
        FROM counties
        WHERE ST_Contains(geometry::geometry, ST_Transform(ST_SetSRID(ST_GeomFromText('POINT(${lons[0]} ${lats[0]})'), 4326), 4269))
      `
    );
  } // latLon

  init(req);
  
  let attributes =
    req.query.attributes?.split(',')
      .map(attr => (
        attr
          .trim()
          .replace(/polygon/i, 'ST_AsText(geometry) as polygon')
      ));
  
  const polygon = req.query.polygon;

  if (!attributes) {
    if (polygon === 'true') {
      attributes = `county,state,state_code,countyfips,statefips,(ST_AsGeoJSON(geometry)::jsonb->'coordinates') as polygonarray,ST_AsText(geometry) as polygon`;
    } else {
      attributes = 'county,state,state_code,countyfips,statefips';
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
} // county

const frost = (req, res) => {
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
          res.status(500).send(err);
        } else if (results.rows.length) {
          res.send(results.rows[0]);
        } else {
          res.send({});
        }
      }
    );
  } // query

  init(req);

  if (location) {
    getLocation(req, query);
  } else {
    query();
  }
}; // frost

module.exports = {
  addresses,
  getAverages,
  getHourly,
  getDaily,
  GAWeatherStations,
  hits,
  indexes,
  tables,
  counttablesrows,
  countindexes,
  databasesize,
  mvm,
  nvm,
  nvm2,
  nvm2Data,
  nvm2Update,
  nvm2Query,
  rosetta,
  watershed,
  mlra,
  county,
  frost,
}
