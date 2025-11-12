import { pool } from 'simple-route';

const nvm2 = async (lat, lon, year) => {
  lat = Math.round(lat);
  lon = Math.round(lon);

  const runNvmQuery = async () => {
    const sq = `
      SELECT n.date, n.lat, n.lon, nldas, COALESCE(mrms, 0) AS mrms
      FROM (
        SELECT date, lat, lon, precipitation AS nldas
        FROM weather
        WHERE (lat, lon) IN (
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
      LEFT JOIN (
        SELECT date, lat, lon, precipitation AS mrms
        FROM weather.mrms_${lat}_${-lon}_${year}
        WHERE (lat, lon) IN (
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
      USING (date, lat, lon)
      ORDER BY 1, 2, 3
    `;

    const { rows: results } = await pool.query(`
      SELECT lat, lon, EXTRACT(month FROM date) AS month, SUM(nldas) AS nldas, SUM(mrms) AS mrms
      FROM (
        ${sq}
      ) alias
      GROUP BY lat, lon, month
      ORDER BY 1, 2, 3
    `);

    let s = `
      <link rel="stylesheet" href="//aesl.ces.uga.edu/weatherapp/src/nvm2.css">
      <script src="https://aesl.ces.uga.edu/scripts/jquery/jquery.js"></script>
      <script src="https://aesl.ces.uga.edu/scripts/jqLibrary.js"></script>
      <script src="https://aesl.ces.uga.edu/weatherapp/src/nvm2.js"></script>
      <script>let monthly = ${JSON.stringify(results)};</script>
      <div id="Data"></div>
    `;

    const { rows: results2 } = await pool.query(`
      SELECT
        TO_CHAR(date, 'yyyy-mm-dd HH:00') AS "Date", lat AS "Lat", lon AS "Lon",
        ROUND(nldas) AS "NLDAS",
        ROUND(mrms)  AS "MRMS",
        ROUND(mrms - nldas) As "&Delta;"
      FROM (
        ${sq}
      ) alias
      WHERE ABS(mrms - nldas) > 13
    `);

    if (results2.rowCount) {
      s += `
        <hr>
        <table id="Flags">
          <thead>
            <tr><th>${Object.keys(results2.rows[0]).join('<th>')}
          </thead>
          <tbody>
            <tr>
              ${results2.rows.map((r) => Object.keys(r).map((v) => `<td>${r[v]}`).join('')).join('<tr>')}
            </tr>
          </tbody>
        </table>
        <hr>
      `;
    }

    pool.query(`
      INSERT INTO nvm2 (lat, lon, year, data)
      VALUES (${lat}, ${lon}, ${year}, '${s.replace(/ /g, ' ').trim()}')
    `);

    return s;
  }; // runNvmQuery

  const { rows } = await pool.query(`
    SELECT data FROM nvm2
    WHERE lat = $1 and lon = $2 and year = $3
  `, [lat, lon, year]);

  if (rows.length) {
    return rows[0].data;
  } else {
    return runNvmQuery();
  }
}; // nvm2

const nvm2Query = async (condition) => {
  const sq = `
    SELECT lat, lon
    FROM nvm2
    WHERE ${condition.replace(/select|insert|update|drop|delete/ig, '')}
    ORDER BY lat, lon
  `;

  const { rows } = await pool.query(sq);
  return rows;
}; // nvm2Query

const nvm2Update = async (red, orange, mismatch, delta, diff, mvm, mrms, lat, lon) => {
  await pool.query(`
    UPDATE nvm2
    SET
      red      = ${red},
      orange   = ${orange},
      mismatch = ${mismatch},
      delta    = ${delta},
      diff     = ${diff},
      mvm      = ${mvm || false},
      mrms     = ${mrms || 'null'}
    WHERE lat = ${lat} AND lon = ${lon}
  `);

  return { status: 'Success' };
}; // nvm2Update

export { nvm2, nvm2Query, nvm2Update };