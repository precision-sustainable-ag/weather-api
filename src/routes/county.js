import { pool } from 'simple-route';

const routeCounty = async (lat, lon, attributes, polygon) => {
  attributes = !attributes ? undefined: attributes.split(',')
    .map((s) => (
      s
        .trim()
        .replace(/polygon/i, '(ST_AsGeoJSON(ST_Multi(geometry))::jsonb->\'coordinates\') as polygonarray, ST_AsText(geometry) as polygon')
    ));

  if (!attributes) {
    if (polygon) {
      attributes = `
        county, state, state_code, countyfips, statefips,
        (ST_AsGeoJSON(ST_Multi(geometry))::jsonb->'coordinates') as polygonarray,
        ST_AsText(geometry) as polygon
      `;
    } else {
      attributes = 'county, state, state_code, countyfips, statefips';
    }
  }

  const { rows } = await pool.query(`
    SELECT distinct ${attributes}
    FROM counties
    WHERE ST_Contains(geometry::geometry, ST_Transform(ST_SetSRID(ST_GeomFromText('POINT(${lon} ${lat})'), 4326), 4269))
  `);

  return rows;
};

export default routeCounty;
