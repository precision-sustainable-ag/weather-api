import { pool } from 'simple-route';

/** ____________________________________________________________________________________________________________________________________
 * Round latitude and longitude values to the nearest NLDAS-2 grid coordinates used in the database.
 *
 * @param {number} n - The latitude or longitude value to be rounded.
 * @returns {string} The rounded NLDAS-2 grid coordinate as a string with three decimal places.
 */
const NLDASlat = (n) => -(Math.floor(-n * 8) / 8).toFixed(3);
const NLDASlon = (n) => (Math.floor(n * 8) / 8).toFixed(3);

/**
 * Restricts a number to a specified range.
 *
 * @param {number} value - The number to be clamped.
 * @param {number} min - The minimum value of the range.
 * @param {number} max - The maximum value of the range.
 * @returns {number} The clamped value.
 */
const clamp = (value, min, max) => Math.min(Math.max(+value, min), max);

/**
 * Creates an array of numbers within a specified range.
 *
 * @param {number} start - The start of the range.
 * @param {number} end - The end of the range (inclusive).
 * @returns {Array<number>} - The array of numbers within the specified range.
 */
const range = (start, end) => {
  const result = [];
  for (let i = start; i <= end; i += 1) {
    result.push(i);
  }

  return result;
}; // range

const yearly = async (lat, lon, year) => {
  const y1 = 2018;
  const y2 = 2022;

  year = year || `${y1}-${y2}`;

  let [year1, year2] = year.toString().split('-');

  year1 = clamp(year1, y1, y2);
  year2 = clamp(year2 || year1, y1, y2);

  // SQL query for fetching yearly temperature data.
  const sq = `
    SELECT
      ${year1}${year2 !== year1 ? ` || '-' || ${year2}` : ''} AS year,
      ${lat} AS lat, ${lon} AS lon,
      min(min_air_temperature) AS min_air_temperature,
      max(max_air_temperature) AS max_air_temperature,
      min(sum_precipitation) AS min_precipitation,
      max(sum_precipitation) AS max_precipitation,
      avg(sum_precipitation) AS avg_precipitation
    FROM (
      ${range(year1, year2).map((y) => `
        SELECT * FROM
        weather.yearly_${Math.trunc(NLDASlat(lat))}_${Math.trunc(-NLDASlon(lon))}_${y}
        WHERE lat=${NLDASlat(lat)} AND lon=${NLDASlon(lon)}
      `).join(`
        UNION ALL
      `)}
    ) a
    GROUP BY lat, lon;
  `;

  const { rows } = await pool.query(sq);
  return rows;
};

export default yearly;