import { pool } from 'simple-route';

import { getLocation } from './query.js';

const watershed = async (lat, lon, attributes, polygon, state, huc, location) => {
  if (location) {
    const results = {};
    await getLocation(location, results);
    lat = results.lats[0];
    lon = results.lons[0];
    console.log({ lat, lon });
  }

  const query = async (sq) => {
    const { rows } = await pool.query(sq);
    return rows.map((row) => {
      delete row.geometry;
      return row;
    });
  }; // query

  attributes = !attributes ? undefined: attributes?.split(',')
    .map((s) => (
      s.trim()
        .replace(/^name$/, 'huc12.name')
        .replace(/huc(\d+)name/, (_, s2) => `huc${s2}.name as huc${s2}name`)
        .replace(/polygon/i, '(ST_AsGeoJSON(ST_Multi(geometry))::jsonb->\'coordinates\') as polygonarray,ST_AsText(geometry) as polygon')
    ));

  const latLon = () => {
    return query(
      `
        SELECT ${attributes}
        FROM huc.huc12
        LEFT JOIN huc.huc10 ON left(huc12.huc12, 10) = huc10.huc10
        LEFT JOIN huc.huc6  ON left(huc12.huc12, 6)  = huc6.huc6
        LEFT JOIN huc.huc8  ON left(huc12.huc12, 8)  = huc8.huc8
        LEFT JOIN huc.huc4  ON left(huc12.huc12, 4)  = huc4.huc4
        LEFT JOIN huc.huc2  ON left(huc12.huc12, 2)  = huc2.huc2
        WHERE ST_Contains(geometry, ST_GeomFromText('POINT(${lon} ${lat})'))
      `,
    );
  }; // latLon

  if (!attributes) {
    if (polygon) {
      attributes = `
        huc12, huc12.name,
        huc10, huc10.name as huc10name,
        huc8, huc8.name as huc8name,
        huc6, huc6.name as huc6name,
        huc4, huc4.name as huc4name,
        huc2, huc2.name as huc2name,
        tnmid, metasourceid, sourcedatadesc, sourceoriginator, sourcefeatureid,
        loaddate, referencegnis_ids, areaacres, areasqkm, states, hutype, humod,
        tohuc, noncontributingareaacres, noncontributingareasqkm, globalid,
        shape_Length, shape_Area,
        (ST_AsGeoJSON(ST_Multi(geometry))::jsonb->'coordinates') as polygonarray,
        ST_AsText(geometry) as polygon
      `;
    } else {
      attributes = `
        huc12, huc12.name,
        huc10, huc10.name as huc10name,
        huc8, huc8.name as huc8name,
        huc6, huc6.name as huc6name,
        huc4, huc4.name as huc4name,
        huc2, huc2.name as huc2name,
        tnmid, metasourceid, sourcedatadesc, sourceoriginator, sourcefeatureid,
        loaddate, referencegnis_ids, areaacres, areasqkm, states, hutype, humod,
        tohuc, noncontributingareaacres, noncontributingareasqkm, globalid,
        shape_Length, shape_Area
      `;
    }
  }

  if (state) {
    return query(
      `
        SELECT ${attributes}
        FROM huc.huc12
        LEFT JOIN huc.huc10 ON left(huc12.huc12, 10) = huc10.huc10
        LEFT JOIN huc.huc6  ON left(huc12.huc12, 6)  = huc6.huc6
        LEFT JOIN huc.huc8  ON left(huc12.huc12, 8)  = huc8.huc8
        LEFT JOIN huc.huc4  ON left(huc12.huc12, 4)  = huc4.huc4
        LEFT JOIN huc.huc2  ON left(huc12.huc12, 2)  = huc2.huc2
        WHERE states like '%${state.toUpperCase()}%'
      `,
    );
  } else if (location) {
    return latLon();
  } else if (huc) {
    return query(
      `
        SELECT ${attributes}
        FROM huc.huc12
        LEFT JOIN huc.huc10 ON left(huc12.huc12, 10) = huc10.huc10
        LEFT JOIN huc.huc6  ON left(huc12.huc12, 6)  = huc6.huc6
        LEFT JOIN huc.huc8  ON left(huc12.huc12, 8)  = huc8.huc8
        LEFT JOIN huc.huc4  ON left(huc12.huc12, 4)  = huc4.huc4
        LEFT JOIN huc.huc2  ON left(huc12.huc12, 2)  = huc2.huc2
        WHERE huc12 like '${huc}%'
      `,
    );
  } else {
    return latLon();
  }
};

export default watershed;