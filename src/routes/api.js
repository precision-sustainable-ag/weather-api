import { pool, makeSimpleRoute } from 'simple-route';

import watershed from './watershed.js';
import routeCounty from './county.js';
import routeMLRA from './mlra.js';
import { nvm2, nvm2Query, nvm2Update } from './nvm2.js';
import yearly from './yearly.js';
import { routeHourly, routeDaily, routeAverages } from './query.js';

export default async function apiRoutes(app) {
  const simpleRoute = makeSimpleRoute(app, pool, { public: true });

  const elevations = {};

  const lat = { type: 'number', required: true, examples: [35],  description: 'Latitude' };
  const lon = { type: 'number', required: true, examples: [-79], description: 'Longitude' };
  const predicted = { type: 'boolean' };
  const explain   = { type: 'boolean' };
  const beta      = { type: 'boolean' };
  const limit     = { type: 'number' };
  const offset    = { type: 'number' };
  const start     = { type: 'string', format: 'date-time', required: true, examples: ['2018-11-01'], description: 'Start date in YYYY-MM-DD format' };
  const end       = { type: 'string', format: 'date-time', required: true, examples: ['2018-11-30'], description: 'End date in YYYY-MM-DD format' };
  const email     = { type: 'string', format: 'email', required: true, examples: ['johndoe@example.com'] };
  
  const polygonarray = {
    type: 'array',
    items: {},
  };

  // Weather -----------------------------------------------------------------------------------------------------------------------
  await simpleRoute('/hourly',
    'Weather',
    'Hourly weather data',
    routeHourly,
    {
      email,
      lat: { description: 'Latitude', examples: [-79] },
      lon: { description: 'Longitude', examples: [-79] },
      start,
      end,
      predicted,
      limit,
      offset,
      explain,
      beta,
    },
    { 200: {} },
  );

  await simpleRoute('/daily',
    'Weather',
    'Daily weather data',
    routeDaily,
    {
      email,
      lat:        { examples: [35] },
      lon:        { examples: [-79] },
      start,
      end,
      predicted,
      limit,
      offset,
      explain,
      beta,
    },
    { 200: {} },
  );

  await simpleRoute('/averages',
    'Weather',
    '5-year hourly weather averages',
    routeAverages,
    {
      email,
      lat: { description: 'Latitude', examples: [-79] },
      lon: { description: 'Longitude', examples: [-79] },
      start: { type: 'string', format: 'date-time', examples: ['2018-11-01'], description: 'Start date in YYYY-MM-DD format' },
      end  : { type: 'string', format: 'date-time', examples: ['2018-11-30'], description: 'End date in YYYY-MM-DD format' },
      predicted,
      limit,
      offset,
      explain,
      beta,
    },
    { 200: {} },
  );
  
  await simpleRoute('/yearlyprecipitation',
    'Weather',
    'Yearly Precipitation',
    `
      SELECT
        lat, lon, rain,
        ST_Distance(geog, ST_SetSRID(ST_MakePoint($2, $1), 4326)) AS distance
      FROM precipitation
      ORDER BY geog <-> ST_SetSRID(ST_MakePoint($2, $1), 4326)
      LIMIT 1
    `,
    { lat, lon },
    { object: true },
  );

  // await simpleRoute('/mvm',
  //   'Weather',
  //   'MRMS vs. MRMS',
  //   async (lat, lon, num) => {
  //     const { rows } = await pool.query(`
  //       SELECT
  //         a.sum - b.sum AS delta,
  //         a.lat AS alat, a.lon AS alon, a.sum AS asum,
  //         b.lat AS blat, b.lon AS blon, b.sum AS bsum
  //       FROM weather.mrms_${lat}_${-lon}_2019_annual a
  //       LEFT JOIN weather.mrms_${lat}_${-lon}_2019_annual b
  //       ON (a.lat BETWEEN b.lat + 0.01 AND b.lat + 0.011 AND
  //           a.lon = b.lon
  //         ) OR (
  //           a.lat = b.lat AND
  //           a.lon BETWEEN b.lon + 0.01 AND b.lon + 0.011
  //         )
  //       WHERE
  //         b.lat IS NOT NULL AND
  //         ABS(a.sum - b.sum) > $1
  //       ORDER BY 1 DESC
  //     `, [num]);
      
  //     return rows;
  //   },
  //   { lat, lon, num: { type: 'number', examples: [50] } },
  //   { numbers: ['delta', 'alat', 'alon', 'asum', 'blat', 'blon', 'bsum'] },
  // );

  await simpleRoute('/frost',
    'Weather',
    'Frost data',
    `
      SELECT * FROM frost
      WHERE 
        firstfreeze IS NOT NULL AND
        firstfrost IS NOT NULL AND
        lastfreeze IS NOT NULL AND
        lastfrost IS NOT NULL AND
        SQRT(POWER(lat - $1, 2) + POWER(lon - $2, 2)) < 0.7
      ORDER BY SQRT(POWER(lat - $1, 2) + POWER(lon - $2, 2))
      LIMIT 1
    `,
    { lat, lon },
    { object: true },
  );

  // await simpleRoute('/yearly',
  //   'Weather',
  //   'Yearly weather aggregates (NLDAS grid)',
  //   yearly,
  //   { lat, lon, year: { examples: [2020] } },
  //   {
  //     numbers: [
  //       'lat', 'lon',
  //       'min_air_temperature', 'max_air_temperature',
  //       'min_precipitation', 'max_precipitation', 'avg_precipitation',
  //     ],
  //     strings: ['year'],
  //   },
  // );

  // Other -----------------------------------------------------------------------------------------------------------------------
  await simpleRoute('/elevation',
    'Other',
    'Elevation',
    async (lat, lon, reply) => {
      lat = lat.toFixed(6);
      lon = lon.toFixed(6);
      const latLon = `${lat} ${lon}`;

      if (!elevations[latLon]) {
        const url = `https://api.open-elevation.com/api/v1/lookup?locations=${lat},${lon}`;
        try {
          const results = await fetch(url);
          if (results.status > 400) {
            reply.code(results.status);
            return results;
          }

          elevations[latLon] = (await results.json()).results[0];
        } catch (ee) {
          reply.code(500).send({ error: ee.message });
        }
      }

      return elevations[latLon];
    },
    { lat, lon },
    {
      numbers: ['latitude', 'longitude', 'elevation'],
      object: true,
    },
  );

  await simpleRoute('/watershed',
    'Other',
    'Watershed',
    watershed,
    {
      lat: { type: 'number', examples: [35] },
      lon: { type: 'number', examples: [-79] },
      polygon: { type: 'boolean' },
    },
    {
      strings: [
        'huc12', 'name', 'huc10', 'huc10name', 'huc8', 'huc8name', 'huc6', 'huc6name', 'huc4', 'huc4name', 'huc2', 'huc2name',
        'tnmid', 'metasourceid', 'sourcedatadesc', 'sourceoriginator', 'sourcefeatureid', 'referencegnis_ids',
        'states', 'hutype', 'humod', 'tohuc', 'globalid', 'polygon',
      ],
      numbers: ['areaacres', 'areasqkm', 'noncontributingareaacres', 'noncontributingareasqkm', 'shape_length', 'shape_area'],
      dates: [ 'loaddate' ],
      other: { polygonarray },      
    },
  );

  await simpleRoute('/county',
    'Other',
    'County',
    routeCounty,
    {
      lat, lon,
      polygon: { type: 'boolean' },
    },
    {
      strings: ['county', 'state', 'state_code', 'countyfips', 'statefips', 'polygon'],
      other: { polygonarray },      
    },
  );

  await simpleRoute('/hardinesszone',
    'Other',
    'Hardiness Zone',
    `
      SELECT
        id, gridcode, zone, trange, zonetitle,
        CASE WHEN $3::boolean
          THEN ST_AsText(geometry)
          ELSE NULL
        END AS polygon,
        CASE WHEN $3::boolean
          THEN ST_AsGeoJSON(ST_Multi(geometry))::jsonb->'coordinates'
          ELSE NULL::jsonb
        END AS polygonarray
      FROM hardiness_zones
      WHERE ST_Intersects(ST_SetSRID(ST_MakePoint($2, $1), 4326), geometry)
    `,
    { lat, lon, polygon: { type: 'boolean' } },
    {
      object: true,
      numbers: ['id', 'gridcode'],
      strings: ['zone', 'trange', 'zonetitle', 'polygon'],
      other: { polygonarray },
    },
  );

  await simpleRoute('/mlra',
    'Other',
    'MLRA',
    routeMLRA,
    {
      lat: { type: 'number', examples: [35] },
      lon: { type: 'number', examples: [-79] },
      polygon: { type: 'boolean' },
    },
    {
      strings: ['name', 'mlrarsym', 'lrrsym', 'lrrname', 'counties', 'states', 'state_codes', 'countyfips', 'statefips', 'polygon'],
      other: { polygonarray },      
    },
  );

  await simpleRoute('/rosetta',
    'Other',
    'Rosetta',
    async (soildata) => {
      const resp = await fetch('https://www.handbook60.org/api/v1/rosetta/1', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ soildata }),
      });
      
      const data = await resp.json();
      return data;
    },
    {
      soildata: {
        type: 'array',
        examples: [
          [
            [30, 30, 40, 1.5, 0.3, 0.1],
            [20, 60, 20],
            [55, 25, 20, 1.1],
          ],
        ],
      },
    },
    {
      method: 'post',
      response: {},
    },
  );

  // Database -----------------------------------------------------------------------------------------------------------------------
  await simpleRoute('/status',
    'Database Utilities',
    'Health check',
    `SELECT 'Connected to database' AS status`,
    undefined,
    { object: true },
  );

  const dbRoutes = {
    DatabaseSize: `SELECT pg_size_pretty(pg_database_size('weatherdb')) AS size`,
    Tables:
      `
        SELECT table_name AS table, reltuples AS rows
        FROM (
          SELECT * FROM information_schema.tables
          WHERE
            table_catalog='weatherdb'
            AND table_name NOT LIKE 'pg%'
            AND table_name NOT LIKE 'sql%'
        ) a
        LEFT JOIN pg_class b
        ON a.table_name = b.relname
        WHERE reltuples > 0
        ORDER BY table_name
      `,
    CountTablesRows:
      `
        SELECT
          COUNT(*) AS tables,
          SUM(reltuples) AS rows
        FROM (
          SELECT * FROM information_schema.tables
          WHERE table_catalog='weatherdb'
        ) a
        LEFT JOIN pg_class b
        ON a.table_name = b.relname
        WHERE reltuples > 0
      `,
    Indexes: `SELECT * FROM pg_indexes WHERE tablename NOT LIKE 'pg%' ORDER BY indexname`,
    CountIndexes: `SELECT COUNT(*) AS indexes FROM pg_indexes WHERE schemaname = 'public'`,
    Addresses: 'SELECT * FROM addresses ORDER BY address',
    Hits:
      `
        SELECT date, ip, query, ms, email
        FROM hits
        WHERE
          query NOT LIKE '%explain%' AND query NOT LIKE '%nvm%' AND
          (date > current_date - 1 OR (ip <> '::ffff:172.18.186.142' AND query NOT LIKE '%25172.18.186%25'))
        ORDER BY date DESC
        LIMIT 1000
      `,
  };

  for (const [route, query] of Object.entries(dbRoutes)) {
    await simpleRoute(`/${route.toLowerCase()}`,
      'Database Utilities',
      route.replace(/[A-Z]/g, (c) => ` ${c}`).trim(),
      query,
    );
  }

  // NVM -----------------------------------------------------------------------------------------------------------------------
  await simpleRoute('/nvm2Data',
    'NLDAS vs. MRMS',
    'NVM data',
    'SELECT DISTINCT lat, lon, year FROM nvm2',
  );

  await simpleRoute('/nvm2',
    'NLDAS vs. MRMS',
    'NVM output',
    nvm2,
    {
      lat, lon,
      year: { type: 'number', examples: [2019] },
    },
    { html: true },
  );

  await simpleRoute('/nvm2Query',
    'NLDAS vs. MRMS',
    'NVM query',
    nvm2Query,
    { condition: { examples: ['mrms IS NOT NULL'] } },
  );

  await simpleRoute('/nvm2Update',
    'NLDAS vs. MRMS',
    'NVM update',
    nvm2Update,
  );
}