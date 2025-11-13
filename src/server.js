import { setup, pool } from 'simple-route';

import { getLocation } from './routes/query.js';
import apiRoutes from './routes/api.js';
import mrvRoutes from './routes/mrv.js';

const TRUSTED_HOSTS = [
  'localhost',
  '127.0.0.1',
  '::1',
  'vegspec.org',                'develop.vegspec.org',
  'covercrop-selector.org',     'develop.covercrop-selector.org',
  'covercrop-seedcalc.org',     'develop.covercrop-seedcalc.org',
  'covercrop-ncalc.org',        'develop.covercrop-ncalc.org',
  'weather.covercrop-data.org', 'developweather.covercrop-data.org',
  'covercrop-imagery.org',      'develop.covercrop-imagery.org',
];

const isTrusted = (req) => {
  const src = req.headers.origin || req.headers.referer || req.headers;
  req.email = req?.query?.email;
  if (src) {
    try {
      const host = new URL(src).hostname;
      if (req.query && !req.query.email) {
        if (/vegspec\.org/.test(host))                      req.email = req.email ?? 'vegspec@psa.org';
        else if (/mrv/.test(host))                          req.email = req.email ?? 'mrv@psa.org';
        else if (/covercrop-selector\.org/.test(host))      req.email = req.email ?? 'selector@psa.org';
        else if (/covercrop-seedcalc\.org/.test(host))      req.email = req.email ?? 'seedcalc@psa.org';
        else if (/covercrop-ncalc\.org/.test(host))         req.email = req.email ?? 'ncalc@psa.org';
        else if (/weather\.covercrop-data\.org/.test(host)) req.email = req.email ?? 'weather@psa.org';
        else if (/covercrop-imagery\.org/.test(host))       req.email = req.email ?? 'imagery@psa.org';
        else if (/localhost/.test(host))                    req.email = req.email ?? 'localhost@psa.org';
      }

      if (TRUSTED_HOSTS.some((h) => host === h || host.endsWith(`.${h}`))) return true;
    } catch { /* ignore */ }
  }
  if (['127.0.0.1', '::1'].includes(req.ip)) {
    req.email = 'localhost@psa.org';
    return true;
  } else {
    return false;
  }
};

const clientIp = (req) => (
  (req.ip ||
   req.headers['cf-connecting-ip'] ||
   req.headers['true-client-ip']   ||
   ''
  ).replace(/^::ffff:/, '')
);

await setup({
  title: 'Weather API',
  version: '2.0.0',
  trusted: ['https://weather.covercrop-data.org/', 'https://developweather.covercrop-data.org/'],
  plugins: {
    '': apiRoutes,
    'mrv': mrvRoutes,
  },
  preValidation: async (req, _reply) => {
    if (!req.query || !req?.routeOptions?.schema?.querystring) return;

    req.startTime = new Date();
    const required = req.routeOptions.schema.querystring.required;
    const properties = req.routeOptions.schema.querystring.properties || {};

    delete req.query.predicted; // deprecated
    if (req.query.output && !properties.output) {
      delete req.query.output;
    }

    if (req.query.location) {
      const location = (req.query.location || '').replace(/[^a-z0-9 ]/ig, '').replace(/\s+/g, ' ').toLowerCase();
      const results = {};
      const rect = /\brect\b/.test(req.query?.options);
      await getLocation(location, results, rect);
      if (rect) {
        req.query.lat = `${results.minLat},${results.maxLat}`;
        req.query.lon = `${results.minLon},${results.maxLon}`;
      } else {
        req.query.lat = results.lats[0];
        req.query.lon = results.lons[0];
      }
      delete req.query.location;
    }

    const trusted = isTrusted(req);
    if (required?.includes('email')) {
      if (!req.query.email && trusted) {
        req.query.email = 'jd@ex.com';
      }
    } else {
      delete req.query.email;
    }
    req.email = req.email ?? req.query?.email;

    const averages = /^\/averages/.test(req.url);
    if (averages) {
      if (!req.query.start) {
        req.query.start = '2099-01-01';
      }
      if (!req.query.end) {
        req.query.end = '2099-12-31';
      }
    }

    if (req.query.attr) {
      req.query.attributes = req.query.attr;
      delete req.query.attr;
    }

    for (const key of ['start', 'end']) {
      if (!req.query?.[key]) {
        continue;
      }

      let v = req.query[key];
      let dt;

      if (Number.isFinite(+v)) {
        dt = new Date();
        dt.setUTCDate(dt.getUTCDate() + +v);
        if (key === 'start') {
          dt.setUTCHours(0, 0, 0, 0);
        } else {
          dt.setUTCHours(23, 59, 59, 999);
        }
      } else {
        v = v.replace(' ', 'T');
        const time = v.split('T')?.[1];

        if (!time) {
          if (key === 'start') {
            v += 'T00:00:00Z';
          } else {
            v += 'T23:59:59Z';
          }
        } else {
          v += 'Z';
        }
  
        if (averages) {
          const day = v.split('-')[2];
          if (!day) {
            v = `2099-${v}`;
          }
          v = v.replace(/-(\d)-/g, (_, c) => `-0${c}-`);
          v = v.replace(/-(\d)T/g, (_, c) => `-0${c}T`);
          dt = new Date(v);
          // dt.setUTCFullYear(2099);
        } else {
          v = v.replace(/-(\d)-/g, (_, c) => `-0${c}-`);
          v = v.replace(/-(\d)T/g, (_, c) => `-0${c}T`);
          dt = new Date(v);
        }
      }

      req.query[key] = dt.toISOString();
    }
  },
  onResponse: async (req, _reply) => {
    if (req.url?.startsWith('/hits')) return;

    const time = new Date() - req.startTime;

    const sql = `
      INSERT INTO public.hits
      (date, ip, query, ms, email)
      VALUES (NOW(), $1, $2, $3, $4)
      RETURNING *;
    `;

    await pool.query(sql, [clientIp(req), req.url, time, req.email]);
    await pool.query('COMMIT;');
  },
});
