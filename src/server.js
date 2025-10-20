import { setup } from 'simple-route';

import apiRoutes from './routes/api.js';

await setup({
  title: 'Weather API',
  version: '1.0.0',
  trusted: ['https://weather.covercrop-data.org/', 'https://developweather.covercrop-data.org/'],
  plugins: {
    '': apiRoutes,
  },
});
