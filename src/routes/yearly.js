import { pool } from 'simple-route';

/** ____________________________________________________________________________________________________________________________________
 * Round latitude and longitude values to the nearest NLDAS-2 grid coordinates used in the database.
 *
 * @param {number} n - The latitude or longitude value to be rounded.
 * @returns {string} The rounded NLDAS-2 grid coordinate as a string with three decimal places.
 */
const NLDASlat = (n) => -(Math.floor(-n * 8) / 8).toFixed(3);
const NLDASlon = (n) => (Math.floor(n * 8) / 8).toFixed(3);

const routeYearly = async (lat, lon, year) => {
  let [year1, year2] = year.toString().split('-');
  if (!year1) {
    year2 = new Date().getFullYear() - 1;
    year1 = year2 - 4;
  } else if (!year2) {
    year2 = year1;
  }

  const start = `${year1}-01-01`;
  const end = `${year2}-12-31 23:59`;

  const sq = `
    SELECT
      ${year1}${year2 !== year1 ? ` || '-' || ${year2}` : ''} AS year,
      ${lat} AS lat, ${lon} AS lon,
      min(min_air_temperature) AS min_air_temperature,
      max(max_air_temperature) AS max_air_temperature,
      avg(avg_air_temperature) AS avg_air_temperature,
      min(sum_precipitation) AS min_precipitation,
      max(sum_precipitation) AS max_precipitation,
      avg(sum_precipitation) AS avg_precipitation
    FROM (
      SELECT
        min(air_temperature) AS min_air_temperature,
        max(air_temperature) AS max_air_temperature,
        avg(air_temperature) AS avg_air_temperature,
        sum(precipitation) AS sum_precipitation,
        lat, lon
      FROM nldas(${NLDASlat(lat)}, ${NLDASlon(lon)}, '${start}', '${end}')
      GROUP BY EXTRACT(year FROM date), lat, lon
    ) a
    GROUP BY lat, lon;
  `;

  const { rows } = await pool.query(sq);
  return rows;
};

export { routeYearly };