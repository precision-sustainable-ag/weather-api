import { pool } from 'simple-route';

const routeMLRA = async (lat, lon, attributes, polygon, mlra) => {
  attributes = !attributes ? undefined : attributes?.split(',')
    .map((s) => (
      s.trim().replace(/polygon/i, '(ST_AsGeoJSON(ST_Multi(geometry))::jsonb->\'coordinates\') as polygonarray,ST_AsText(geometry) as polygon')
    ));

  const latLon = async () => {
    if (mlra) {
      const { rows } = await pool.query(`
        SELECT
          b.name, b.mlrarsym, b.lrrsym, b.lrrname,
          STRING_AGG(DISTINCT county || ' County' || ' ' || state, ', ') as counties,
          STRING_AGG(DISTINCT state, ', ') as states,
          STRING_AGG(DISTINCT state_code,', ') as state_codes,
          STRING_AGG(DISTINCT countyfips, ', ') as countyfips,
          STRING_AGG(DISTINCT statefips, ', ') as statefips
          ${polygon ? ', polygon' : ''}
        FROM counties a
        RIGHT JOIN (
          SELECT *, ST_AsText(geometry) as polygon
          FROM mlra2022
          WHERE mlrarsym = '${mlra}'
        ) b
        ON ST_Intersects(ST_SetSRID(b.geometry, 4269), a.geometry)
        GROUP BY b.name,b.mlrarsym,b.lrrsym,b.lrrname ${polygon ? ', polygon' : ''}
      `);

      return rows;
    } else {
      const { rows } = await pool.query(`
        SELECT distinct ${attributes}
        FROM mlra2022
        WHERE ST_Contains(geometry, ST_GeomFromText('POINT(${lon} ${lat})'))
      `);

      return rows;
    }
  }; // latLon

  if (!attributes) {
    if (polygon) {
      attributes = `
        name, mlrarsym, lrrsym, lrrname,
        (ST_AsGeoJSON(ST_Multi(geometry))::jsonb->'coordinates') as polygonarray,
        ST_AsText(geometry) as polygon
      `;
    } else {
      attributes = 'name, mlrarsym, lrrsym, lrrname';
    }
  }

  return latLon();
};

export default routeMLRA;
