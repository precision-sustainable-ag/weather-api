import { pool, makeSimpleRoute } from 'simple-route';

export default async function mrvRoutes(app) {
  const simpleRoute = makeSimpleRoute(app, pool, { public: true });

  await simpleRoute('/categories',
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

  await simpleRoute('/setcategory',
    'MRV',
    'MRV Set category',
    async (state, date, field, category) => {
      await pool.query(`
        DELETE FROM mrv
        WHERE
          state = $1
          AND TO_CHAR(date, 'YY-MMDD') = $2
          AND field = $3
      `, [state, date, field]);

      if (category) {
        await pool.query(`
          INSERT INTO mrv
          (state, date, field, category)
          VALUES ($1, TO_DATE($2, 'YY-MMDD'), $3, $4)
        `, [state, date, field, category]);
      }

      return { status: 'Success' };
    },
    undefined,
    { 200: {} },
  );
}