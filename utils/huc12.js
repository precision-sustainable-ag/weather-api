// downloaded from https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Hydrography/WBD/National/GDB/
// converted to CSV per https://gis.stackexchange.com/questions/381340/gdbtable-file-type-how-gis-reads-it-and-how-to-batch-convert-it-to-a-csv

'use strict';

const fs = require('fs');
const {pool} = require('../API/pools');

const readline = require('readline');

async function readFile() {
  const fileStream = fs.createReadStream('huc12.csv');

  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });
  // Note: we use the crlfDelay option to recognize all instances of CR LF
  // ('\r\n') in input.txt as a single line break.

  const dec = 6;
  let next = '';
  let first = true;

  for await (let line of rl) {
    if (first) {
      first = false;
      continue;
    }

    if (!/\)\)"$/.test(line)) {
      next += line;
      continue;
    } else if (next) {
      line = next + line;
      next = '';
      console.log(line.slice(0, 200));
      console.log('_'.repeat(200));
    }

    line = line.replace(/"[^"]+?"/g, (c) => c.replace(/,/g, ' '));

    if (true) {
      let [id,tnmid,metasourceid,sourcedatadesc,sourceoriginator,sourcefeatureid,loaddate,referencegnis_ids,areaacres,areasqkm,states,huc12,name,hutype,humod,tohuc,noncontributingareaacres,noncontributingareasqkm,globalid,shape_Length,shape_Area,geometry] = line.split(',');

      name = name.replace(/'/g, `''`);

      // console.log({id,tnmid,metasourceid,sourcedatadesc,sourceoriginator,sourcefeatureid,loaddate,referencegnis_ids,areaacres,areasqkm,states,huc12,name,hutype,humod,tohuc,noncontributingareaacres,noncontributingareasqkm,globalid,shape_Length,shape_Area});
      
      geometry = geometry.replace(/\d+\.\d+/g, d => (+d).toFixed(dec));
      geometry = geometry.replace(/  /g, ', ');
      // geometry = geometry.replace('"MULTIPOLYGON (((', '').replace(')))"', '');
      geometry = geometry.replace(/"/g, '');

      const sq = `
        insert into huc.huc12 (tnmid, metasourceid, sourcedatadesc, sourceoriginator, sourcefeatureid, loaddate, referencegnis_ids, areaacres, areasqkm, states, huc12, name, hutype, humod, tohuc, noncontributingareaacres, noncontributingareasqkm, globalid, shape_Length, shape_Area, geometry)
        values (
          '${tnmid}', '${metasourceid}', 
          '${sourcedatadesc}', '${sourceoriginator}', '${sourcefeatureid}', '${loaddate}', '${referencegnis_ids}', '${areaacres}', '${areasqkm}', '${states}', '${huc12}', '${name}', '${hutype}', '${humod}', '${tohuc}', '${noncontributingareaacres}', '${noncontributingareasqkm}', '${globalid}', '${shape_Length}', '${shape_Area}',
          ST_GeometryFromText('${geometry}')
        );
      `.replace(/"/g, '');
      
      // console.log(sq);
      await pool.query(sq, (err, results) => {
        if (err) {
          console.log('ERROR');
          console.log(geometry);
          console.log('_'.repeat(200));
          // console.log(sq);
          console.log(err);
          // console.log(n);
          throw '';
        }
      });
      // break;
    }
  }
}

readFile();