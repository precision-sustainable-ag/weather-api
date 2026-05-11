import { makeSimpleRoute, pool } from 'simple-route';

export default async function mapRoutes(app) {
  const simpleRoute = makeSimpleRoute(app, pool, { public: true });

  await simpleRoute(
    '/savecoords',
    'map',
    'Save Map Coordinates',
    async (user, lat, lon) => {
      console.log({ user, lat, lon });
      await pool.query(
        `
        DELETE FROM marker_locations
        WHERE user_id=$1
      `,
        [user],
      );

      await pool.query(
        `
        INSERT INTO marker_locations (user_id, lat, lon)
        VALUES ($1, $2, $3)
      `,
        [user, lat, lon],
      );
    },
    {},
    {
      method: 'POST',
    },
  );

  await simpleRoute(
    '/loadcoords',
    'map',
    'Load Map Coordinates',
    `
      SELECT lat, lon
      FROM marker_locations
      WHERE user_id = $1
    `,
    {
      user: { type: 'string' },
    },
  );
}
