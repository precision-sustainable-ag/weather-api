const fs = require('fs');
const {pool} = require('../API/pools');

const readFiles = (dirname, onFileContent, onError = () => {}) => {
  fs.readdir(dirname, function(err, filenames) {
    if (err) {
      console.log(err);
      onError(err);
      return;
    }
    filenames.forEach(function(filename) {
      fs.readFile(dirname + filename, 'utf-8', function(err, content) {
        if (err) {
          onError(err);
          return;
        }
        onFileContent(filename, content);
      });
    });
  });
};

const readFile = (filename, content) => {
  const get = (parm) => {
    return (content[1][content[0].indexOf(parm)] || 'NULL')
      .replace(/'/g, `''`)
      .replace(/COMMA/g, ',')
      .replace('-9999.0', 'NULL');
  } // get

  content = content.replace(/"\s*([^"]+?)\s*"/g, (_, c) => c.replace(/,/g, 'COMMA').trim());
  content = content.split(/[\n\r]+/).map(s => s.split(','));
  if (content[0].length != content[1].length) {
    console.log(content[0].length, content[1].length);
  }
  
  const station = content[1][0];
  const lat         = get('LATITUDE');
  const lon         = get('LONGITUDE');
  const name        = get('NAME');
  const firstFreeze = get('ANN-TMIN-PRBFST-T32FP50');
  const firstFrost  = get('ANN-TMIN-PRBFST-T36FP50');
  const lastFreeze  = get('ANN-TMIN-PRBLST-T32FP50');
  const lastFrost   = get('ANN-TMIN-PRBLST-T36FP50');

  const sq = `
    insert into frost.frost (station, lat, lon, name, firstFreeze, firstFrost, lastFreeze, lastFrost)
    values ('${station}', ${lat}, ${lon}, '${name}', '${firstFreeze}', '${firstFrost}', '${lastFreeze}', '${lastFrost}');
  `;
  
  pool.query(sq, (err, results) => {
    if (err) {
      console.log(sq);
      throw '';
    }
  });
};

pool.query(`
  drop table if exists frost.frost;

  create table frost.frost (
    lat real,
    lon real,
    station text,
    name text,
    firstFreeze text,
    firstFrost text,
    lastFreeze text,
    lastFrost text
  );
`);

readFiles('./csv/', readFile);