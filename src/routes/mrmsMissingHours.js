import fs from 'node:fs/promises';

const hourMs = 60 * 60 * 1000;

// mrmsYYYYMMDDHH.gz or omrmsYYYYMMDDHH.gz
const mrmsRe = /^(?:o?mrms)(\d{4})(\d{2})(\d{2})(\d{2})\.gz$/;

/**
 * Return array of missing MRMS hourly timestamps (as ISO strings)
 * between `start` and `end` (inclusive), based on filenames in `dir`.
 *
 * @param {string} dir - Directory containing MRMS .gz files
 * @param {Object} options
 * @param {Date} [options.start] - Start datetime (UTC). Default 2015-01-01 00:00Z
 * @param {Date} [options.end]   - End datetime (UTC). Default: now (truncated to hour)
 * @returns {Promise<string[]>}  - Array of ISO timestamps for missing hours
 */
export async function getMissingMrmsHours(dir, options = {}) {
  const entries = await fs.readdir(dir);

  const existingHours = new Set();

  for (const name of entries) {
    const match = mrmsRe.exec(name);
    if (!match) continue;

    const [, y, m, d, h] = match;
    const ts = Date.UTC(+y, +m - 1, +d, +h); // UTC hour
    existingHours.add(ts);
  }

  if (!existingHours.size) {
    // No files at all – all hours in the range are "missing"
    // We still compute range below, Set will just be empty.
  }

  const startDate = options.start ?? new Date(Date.UTC(2015, 0, 1, 0));
  const endDate = options.end ?? new Date();

  // Truncate both to the hour (UTC)
  const startTs = Date.UTC(
    startDate.getUTCFullYear(),
    startDate.getUTCMonth(),
    startDate.getUTCDate(),
    startDate.getUTCHours(),
  );

  const endTs = Date.UTC(
    endDate.getUTCFullYear(),
    endDate.getUTCMonth(),
    endDate.getUTCDate(),
    endDate.getUTCHours(),
  );

  const missing = [];

  for (let ts = startTs; ts <= endTs; ts += hourMs) {
    if (!existingHours.has(ts)) {
      missing.push(new Date(ts).toISOString());
    }
  }

  return missing;
}
