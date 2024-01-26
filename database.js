const { pool } = require('./pools');

const sendResults = (req, res, results, opt = {}) => {
  if (req.testing) {
    if (typeof results === 'object') {
      res.write('SUCCESS');
    } else {
      res.write(results);
    }
    // res.write(`\n${'_'.repeat(200)}\n`);

    if (!req.tests.length) {
      res.write('\nFinished');
      req.testing = false;
      res.end();
    } else {
      req.testing = true;
      res.write(`\n${req.tests[0].name.padEnd(25)}: `);
      req.tests.shift()(req, req.testResponse);
    }
  } else if (req.query.output === 'html') {
    if (!Array.isArray(results)) {
      results = [results];
    }

    res.send(`
      <link rel="stylesheet" href="/css/dbGraph.css">
      <link rel="stylesheet" href="/css/weather.css">
      <style>
        table {
          position: relative;
          overflow: hidden;
        }

        tr {
          vertical-align: top;
          position: relative;
        }

        tr.even {
          background: #efc;
        }

        td:nth-child(1)[rowspan] {
          position: relative;
        }

        tr.even td:nth-child(1)::before {
          content: '';
          position: absolute;
          height: 100%;
          top: 0;
          left: 0;
          width: 100vw;
          outline: 1px solid #666;
          z-index: 999;
        }

        a {
          position: absolute;
          z-index: 1000;
        }
      </style>

      <div id="Graph"></div>

      <table id="Data">
        <thead>
          <tr><th>${Object.keys(results[0]).join('<th>')}</tr>
        </thead>
        <tbody>
          ${results.map((r) => `<tr><td>${Object.keys(r).map((v) => r[v]).join('<td>')}`).join('\n')}
        </tbody>
      </table>

      ${opt.rowspan ? `
        <script>
          const data = document.querySelector('#Data tbody');
          let cname = 'odd';
          [...data.rows].forEach((r1, i) => {
            for (let n = 0; n < data.rows[0].cells.length; n++) {
              if (n === 0 && r1.cells[0].style.display) continue;

              if (n === 0 && !r1.className) r1.classList.add(cname);

              for (let j = i + 1; j < data.rows.length; j++) {
                if ((n > 0) && (j - i + 1 > (r1.cells[0].rowSpan || 1))) {
                  break;
                }
                const r2 = data.rows[j];
                if (r1?.cells[n]?.innerText === r2?.cells[n]?.innerText) {
                  if (n === 0) r2.classList.add(cname);
                  r1.cells[n].rowSpan = j - i + 1;
                  r2.cells[n].style.display = 'none';
                } else {
                  break;
                }
              }
              
              if ((n === 0) && !r1.cells[0].style.display) {
                cname = cname === 'odd' ? 'even' : 'odd';
              }
            }
          });
        </script>` : ''}
    `);
  } else if (req.query.output === 'csv') {
    if (!Array.isArray(results)) {
      results = [results];
    }

    const s = `${Object.keys(results[0]).toString()}\n${
      results.map((r) => Object.keys(r).map((v) => r[v])).join('\n')}`;

    res.set('Content-Type', 'text/csv');
    res.setHeader('Content-disposition', `attachment; filename=${req.query.lat}.${req.query.lon}.csv`);
    res.send(s);
  } else {
    res.send(results);
  }
}; // sendResults

/**
 * Logs a message along with the line number where the debug function is called.
 * @param {string} s - The message to log.
 * @returns {void}
 */
const debug = (s, req, res, status = 200) => {
  try {
    throw new Error();
  } catch (error) {
    // Extract the stack trace
    const stackLines = error.stack.split('\n');

    let lineNumber;
    // Find the line number
    try {
      lineNumber = parseInt(stackLines[2].match(/at.*\((.*):(\d+):\d+\)/)[2], 10);
    } catch (err) {
      lineNumber = '';
    }

    const result = `
      Line ${lineNumber}
${JSON.stringify(s, null, 2).replace(/\\n/g, '\n')}
    `.trim();

    console.log(result);
    console.log('_'.repeat(process.stdout.columns));

    if (res && !req.testing) {
      res.type('text/plain');
      res.status(status).send(result);
    } else if (res && req.testing) {
      sendResults(req, res, `ERROR\n${result}\n`);
    }
  }
}; // debug

const simpleQuery = (sq, parameters, req, res, hideUnused) => {
  pool.query(
    sq,
    parameters,
    (err, results) => {
      if (hideUnused) {
        const used = new Set();
        results.rows.forEach((row) => {
          Object.keys(row).filter((key) => row[key] !== null && row[key] !== '').forEach((key) => used.add(key));
        });
        results.rows.forEach((row) => {
          Object.keys(row).filter((key) => !used.has(key)).forEach((key) => delete row[key]);
        });
      }

      if (err) {
        debug(err, req, res, 500);
      } else if (results.rows.length) {
        sendResults(req, res, results.rows);
      } else {
        sendResults(req, res, {});
      }
    },
  );
}; // simpleQuery

/** ____________________________________________________________________________________________________________________________________
 * Sanitizes a string for safe use in an SQL query.
 *
 * @param {string} s - The string to sanitize.
 * @returns {string} - The sanitized string.
 */
const sanitize = (s) => (
  (s || '')
    .replace(/\b(select|insert|update|drop|delete|truncate|create|alter|grant|revoke)\b/ig, '')
    .replace(/'/g, `''`)
); // sanitize

/** ____________________________________________________________________________________________________________________________________
 * Sanitizes a query parameter by converting it to a safe SQL string.
 * Works for both POST and GET.
 *
 * @param {object} req - The request object from Express.js.
 * @param {string} parm - The name of the query parameter to sanitize.
 * @returns {string} A sanitized SQL string.
 */
const safeQuery = (req, parm) => (
  sanitize(req.body[parm] || req.query[parm])
); // safeQuery

module.exports = {
  sendResults,
  debug,
  simpleQuery,
  safeQuery,
};
