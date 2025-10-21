import { pool, makeSimpleRoute, schema200 } from 'simple-route';

import watershed from './watershed.js';
import routeCounty from './county.js';
import routeMLRA from './mlra.js';

export default async function apiRoutes(app) {
  const simpleRoute = makeSimpleRoute(app, pool, { public: true });

  // -----------------------------------------------------------------------------------------------------------------------
  await simpleRoute('/status',
    'Database',
    'Health check',
    `SELECT 'Connected to database' AS status`,
    undefined,
    { object: true },
  );

  // Weather -----------------------------------------------------------------------------------------------------------------------

  const lat = { type: 'number', required: true, examples: [35] };
  const lon = { type: 'number', required: true, examples: [-79] };

  const dbRoutes = {
    DatabaseSize: `SELECT pg_size_pretty(pg_database_size('postgres')) AS size`,
    Tables:
      `
        SELECT table_name AS table, reltuples AS rows
        FROM (
          SELECT * FROM information_schema.tables
          WHERE table_schema='weather'
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
          WHERE table_schema='weather'
        ) a
        LEFT JOIN pg_class b
        ON a.table_name = b.relname
        WHERE reltuples > 0
      `,
    Indexes: `SELECT * FROM pg_indexes WHERE tablename NOT LIKE 'pg%' ORDER BY indexname`,
    CountIndexes: `SELECT COUNT(*) AS indexes FROM pg_indexes WHERE schemaname = 'weather'`,
    Addresses: 'SELECT * FROM weather.addresses ORDER BY address',
    Hits:
      `
        SELECT date, ip, query, ms, email
        FROM weather.hits
        WHERE
          query NOT LIKE '%explain%' AND query NOT LIKE '%nvm%' AND
          (date > current_date - 1 OR (ip <> '::ffff:172.18.186.142' AND query NOT LIKE '%25172.18.186%25'))
        ORDER BY date DESC
        LIMIT 1000
      `,
  };

  for (const [route, query] of Object.entries(dbRoutes)) {
    await simpleRoute(`/${route.toLowerCase()}`,
      'Database',
      route.replace(/[A-Z]/g, (c) => ` ${c}`).trim(),
      query,
    );
  };

  await simpleRoute('/hardinesszone',
    'Weather',
    'Hardiness Zone',
    `
      SELECT
        id, gridcode, zone, trange, zonetitle,
        CASE WHEN $3::boolean
          THEN (ST_AsGeoJSON(ST_Multi(geometry))::jsonb->'coordinates')
          ELSE NULL
        END AS polygonarray
      FROM hardiness_zones
      WHERE ST_Intersects(ST_SetSRID(ST_MakePoint($2, $1), 4326), geometry)
    `,
    {
      lat,
      lon,
      polygon: { type: 'boolean' },
    },
  );

  await simpleRoute('/yearlyprecipitation',
    'Weather',
    'Yearly Precipitation',
    `
      SELECT
        lat, lon, rain,
        ST_Distance(geog, ST_SetSRID(ST_MakePoint($2, $1), 4326)) AS distance
      FROM weather.precipitation
      ORDER BY geog <-> ST_SetSRID(ST_MakePoint($2, $1), 4326)
      LIMIT 1
    `,
    {
      lat,
      lon,
    },
  );

  await simpleRoute('/mvm',
    'Weather',
    'MVM',
    async (lat, lon, num) => {
      const { rows } = await pool.query(`
        SELECT
          a.sum - b.sum AS delta,
          a.lat AS alat, a.lon AS alon, a.sum AS asum,
          b.lat AS blat, b.lon AS blon, b.sum AS bsum
        FROM weather.mrms_${lat}_${-lon}_2019_annual a
        LEFT JOIN weather.mrms_${lat}_${-lon}_2019_annual b
        ON (a.lat BETWEEN b.lat + 0.01 AND b.lat + 0.011 AND
            a.lon = b.lon
          ) OR (
            a.lat = b.lat AND
            a.lon BETWEEN b.lon + 0.01 AND b.lon + 0.011
          )
        WHERE
          b.lat IS NOT NULL AND
          ABS(a.sum - b.sum) > ${num}
        ORDER BY 1 DESC
      `);
      
      return rows;
    },
    {
      lat, lon,
      num: { type: 'number', examples: [50] },
    },
    await schema200(pool, `SELECT 0.0 AS delta, 0.0 AS alat, 0.0 AS alon, 0.0 AS asum, 0.0 AS blat, 0.0 AS blon, 0.0 AS bsum `),
  );

  await simpleRoute('/nvm2Data',
    'NVM',
    'NVM data',
    'SELECT DISTINCT lat, lon, year FROM weather.nvm2',
  );

  await simpleRoute('/frost',
    'Weather',
    'Frost data',
    `
      SELECT * FROM frost.frost
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

  // MRV -----------------------------------------------------------------------------------------------------------------------
  await simpleRoute('/mrv/categories',
    'MRV',
    'MRV Categories',
    `
      SELECT field, category
      FROM mrv
      WHERE UPPER(state) = UPPER($1) AND TO_CHAR(date, 'YY-MMDD') = $2
      ORDER BY field
    `,
    {
      state: { examples: ['SPAINCALIBRACIONPRISMA2025'] },
      date: { examples: ['25-0625'] },
    },
  );

  // Other -----------------------------------------------------------------------------------------------------------------------
  const elevations = {};
  await simpleRoute('/elevation',
    'Other',
    'Elevation',
    async (lat, lon) => {
      lat = lat.toFixed(6);
      lon = lon.toFixed(6);
      const latLon = `${lat} ${lon}`;

      if (!elevations[latLon]) {
        const url = `https://api.open-elevation.com/api/v1/lookup?locations=${lat},${lon}`;
        elevations[latLon] = (await (
          await fetch(url)
        ).json()).results[0];
        console.log('fetched', url, JSON.stringify(elevations[latLon]));
      }

      return elevations[latLon];
    },
    { lat, lon },
    { object: true },
  );

  await simpleRoute('/watershed',
    'Other',
    'Watershed',
    async (lat, lon, attributes, polygon, state, huc, location) => (
      await watershed(lat, lon, attributes, polygon, state, huc, location)
    ),
    {
      lat: { type: 'number', examples: [35] },
      lon: { type: 'number', examples: [-79] },
      polygon: { type: 'boolean' },
    },
    { response: {} },
  );

  await simpleRoute('/county',
    'Other',
    'County',
    async (lat, lon, attributes, polygon) => (
      routeCounty(lat, lon, attributes, polygon)
    ),
    {
      lat, lon,
      polygon: { type: 'boolean' },
    },
    { response: {} },
  );

  await simpleRoute('/mlra',
    'Other',
    'MLRA',
    async (lat, lon, attributes, polygon, mlra) => (
      routeMLRA(lat, lon, attributes, polygon, mlra)
    ),
    {
      lat: { type: 'number', examples: [35] },
      lon: { type: 'number', examples: [-79] },
      polygon: { type: 'boolean' },
    },
    { response: {} },
  );  
}