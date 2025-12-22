import { pool, makeSimpleRoute } from 'simple-route';

import watershed from './watershed.js';
import routeCounty from './county.js';
import routeMLRA from './mlra.js';
import { nvm2, nvm2Query, nvm2Update } from './nvm2.js';
import { routeHourly, routeDaily, routeAverages } from './query.js';
import { routeYearly } from './yearly.js';

const database = process.env.DB_DATABASE;

export default async function apiRoutes(app) {
  const simpleRoute = makeSimpleRoute(app, pool, { public: true });

  const elevations = {};

  const lat = { type: 'number', required: true, examples: [35],  description: 'Latitude' };
  const lon = { type: 'number', required: true, examples: [-79], description: 'Longitude' };
  const explain   = { type: 'boolean' };
  const limit     = { type: 'number' };
  const offset    = { type: 'number' };
  const start     = { type: 'string', format: 'date-time', required: true, examples: ['2018-11-01'], description: 'Start date in YYYY-MM-DD format' };
  const end       = { type: 'string', format: 'date-time', required: true, examples: ['2018-11-30'], description: 'End date in YYYY-MM-DD format' };
  const email     = { type: 'string', format: 'email', required: true, examples: ['jd@ex.com'] };
  
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
      limit,
      offset,
      explain,
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
      limit,
      offset,
      explain,
    },
    { 200: {} },
  );

  await simpleRoute('/averages',
    'Weather',
    '5-year hourly weather averages',
    routeAverages,
    {
      email,
      lat: { description: 'Latitude', examples: [35] },
      lon: { description: 'Longitude', examples: [-79] },
      start: { type: 'string', format: 'date-time', examples: ['2018-11-01'], description: 'Start date in YYYY-MM-DD format' },
      end  : { type: 'string', format: 'date-time', examples: ['2018-11-30'], description: 'End date in YYYY-MM-DD format' },
      limit,
      offset,
      explain,
    },
    { 200: {} },
  );

  await simpleRoute('/yearly',
    'Weather',
    'Yearly weather data',
    routeYearly,
    {
      email,
      lat: { description: 'Latitude', examples: [35] },
      lon: { description: 'Longitude', examples: [-79] },
      year: { type: 'string', examples: ['2020', '2018-2020'], description: 'Year or range of years (e.g., 2018-2020)' },
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
    `
      SELECT
        'Connected to database' AS status,
        last_nldas_import,
        ROUND(EXTRACT(EPOCH FROM (NOW() - last_nldas_import)) / 3600) AS hours_since_last_nldas,
        last_mrms_import,
        ROUND(EXTRACT(EPOCH FROM (NOW() - last_mrms_import)) / 3600) AS hours_since_last_mrms
      FROM status`,
    undefined,
    { object: true },
  );

  const dbRoutes = {
    DatabaseSize: `SELECT pg_size_pretty(pg_database_size('${database}')) AS pretty_size, pg_database_size('${database}') AS size`,
    Tables:
      `
        SELECT
          row_number() OVER (ORDER BY table_name),
          table_name AS table,
          reltuples AS rows,
          to_char(reltuples, 'FM999G999G999G999G999') AS pretty_rows
        FROM (
          SELECT * FROM information_schema.tables
          WHERE
            table_catalog='${database}'
            AND table_name NOT LIKE 'pg%'
            AND table_name NOT LIKE 'sql%'
        ) a
        LEFT JOIN pg_class b
        ON a.table_name = b.relname
        WHERE reltuples > 0
      `,
    CountTablesRows:
      `
        SELECT
          COUNT(*) AS tables,
          SUM(reltuples) AS rows,
          to_char(SUM(b.reltuples)::numeric, 'FM999G999G999G999G999') AS pretty_rows
        FROM (
          SELECT * FROM information_schema.tables
          WHERE table_catalog='${database}'
        ) a
        LEFT JOIN pg_class b
        ON a.table_name = b.relname
        WHERE reltuples > 0
      `,
    Indexes: 
      `
        SELECT
          row_number() OVER (ORDER BY ix.indexname),
          ix.schemaname,
          ix.tablename,
          ix.indexname,
          pg_size_pretty(pg_relation_size(i.oid))        AS index_size,
          pg_size_pretty(pg_total_relation_size(i.oid))  AS index_total_size, -- incl. TOAST/etc
          s.idx_scan,
          s.idx_tup_read,
          s.idx_tup_fetch,
          ix.indexdef
        FROM pg_indexes ix
        JOIN pg_class      AS i ON i.relname = ix.indexname
        JOIN pg_namespace  AS n ON n.oid = i.relnamespace AND n.nspname = ix.schemaname
        LEFT JOIN pg_stat_all_indexes AS s
          ON s.schemaname = ix.schemaname AND s.relname = ix.tablename AND s.indexrelid = i.oid
        WHERE ix.tablename NOT LIKE 'pg%'
      `,
    CountIndexes: `SELECT COUNT(*) AS indexes FROM pg_indexes WHERE schemaname = 'public'`,
    Addresses: 'SELECT row_number() OVER (ORDER BY address), * FROM addresses',
    Running_Queries:
      `
        SELECT pid, state, (now() - query_start)::text AS runtime, query
        FROM pg_stat_activity
        WHERE state <> 'idle' AND query NOT LIKE '%idle%'
      `,
    Partitions:
      `
        SELECT
          n.nspname AS schema,
          c.relname AS parent,
          COUNT(t.relid) AS leaf_count,
          SUM(pg_total_relation_size(t.relid)) AS bytes,
          pg_size_pretty(SUM(pg_total_relation_size(t.relid))) AS total_size
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN LATERAL (
          SELECT relid
          FROM pg_partition_tree(c.oid)
          WHERE isleaf
        ) t ON true
        WHERE c.relkind = 'p'
        GROUP BY 1, 2
        ORDER BY bytes DESC NULLS LAST
      `,
  };

  for (const [route, query] of Object.entries(dbRoutes)) {
    await simpleRoute(`/${route.toLowerCase()}`,
      'Database Utilities',
      route.replace(/[A-Z]/g, (c) => ` ${c}`).trim(),
      query,
    );
  }

  await simpleRoute('/hits',
    'Database Utilities',
    'API Hits',
    `
      SELECT
        date, ip, ms,
        regexp_replace(
          query,
          '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+',
          '[redacted]',
          'gi'
        ) AS query
      FROM hits
      WHERE
        query NOT LIKE '%explain%' AND query NOT LIKE '%nvm%' AND
        (date > now() - LEAST($2, 30) * interval '1 day')
        AND ip <> '127.0.0.1'
      ORDER BY
        CASE
          WHEN $1 = 'ms' THEN ms
          ELSE NULL
        END DESC,
        CASE
          WHEN $1 IS NULL OR $1 <> 'ms'
            THEN date
          ELSE NULL
        END DESC
    `,
    {
      order: { type: 'string', enum: ['date', 'ms'], examples: ['date'] },
      days: { type: 'number', default: 14, examples: [7], description: 'Number of days to look back from today' },
    },
  );

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