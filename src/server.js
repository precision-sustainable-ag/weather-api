import { setup } from 'simple-route';

import apiRoutes from './routes/api.js';
import mrvRoutes from './routes/mrv.js';

await setup({
  title: 'Weather API',
  version: '1.0.0',
  trusted: ['https://weather.covercrop-data.org/', 'https://developweather.covercrop-data.org/'],
  plugins: {
    '': apiRoutes,
    'mrv': mrvRoutes,
  },
  preValidation: (req, _reply, done) => {
    for (const key of ['start', 'end']) {
      const v = req.query?.[key];
      if (typeof v === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(v)) {
        req.query[key] = v + (key === 'end' ? 'T23:59:59Z' : 'T00:00:00Z');
      } else if (Number.isFinite(+v)) {
        const d = new Date();
        d.setDate(d.getDate() + +v);
        req.query[key] = d.toISOString();
      }
    }
    done();
  },
});
