// mrms-missing-hours.js
// Usage: node mrms-missing-hours.js /path/to/folder

const fs = require('fs').promises;

(async () => {
  const dir = process.argv[2] || '.';

  // mrmsYYYYMMDDHH.gz or omrmsYYYYMMDDHH.gz
  const re = /^(?:o?mrms)(\d{4})(\d{2})(\d{2})(\d{2})\.gz$/;

  const entries = await fs.readdir(dir);

  // Set of existing hourly timestamps (ms since epoch, UTC)
  const existingHours = new Set();
  for (const name of entries) {
    const match = re.exec(name);
    if (!match) continue;

    const [, y, m, d, h] = match;
    const ts = Date.UTC(+y, +m - 1, +d, +h); // UTC hour
    existingHours.add(ts);
  }

  if (!existingHours.size) {
    console.error('No MRMS files found in', dir);
    process.exit(1);
  }

  const hourMs = 60 * 60 * 1000;

  // Start: fixed 2015-01-01 00:00 UTC
  const startTs = Date.UTC(2015, 0, 1, 0);

  // End: current hour (UTC), truncated to the hour
  const now = new Date();
  const endTs = Date.UTC(
    now.getUTCFullYear(),
    now.getUTCMonth(),
    now.getUTCDate(),
    now.getUTCHours(),
  );

  const pad = n => String(n).padStart(2, '0');
  const fmt = ts => {
    const d = new Date(ts);
    return (
      d.getUTCFullYear() +
      '-' + pad(d.getUTCMonth() + 1) +
      '-' + pad(d.getUTCDate()) +
      ' ' + pad(d.getUTCHours()) + 'Z'
    );
  };

  const missingDates = []; // array of real Date objects for missing hours

  for (let ts = startTs; ts <= endTs; ts += hourMs) {
    if (!existingHours.has(ts)) {
      missingDates.push(new Date(ts));
    }
  }

  console.error(
    `From ${fmt(startTs)} to ${fmt(endTs)}:` +
      `\n  Existing hours: ${existingHours.size}` +
      `\n  Missing hours: ${missingDates.length}`,
  );

  // Final array of missing hours as Date objects:
  // (change to .toISOString() if you prefer strings)
  console.log(missingDates.length);
  // console.log(
  //   JSON.stringify(
  //     missingDates.map(d => d.toISOString()),
  //     null,
  //     2
  //   )
  // );
})();
