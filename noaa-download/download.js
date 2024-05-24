const axios = require('axios');
const cheerio = require('cheerio');
const fs = require('fs');
const path = require('path');

const baseURL = 'https://www.ncei.noaa.gov/data/normals-annualseasonal/2006-2020/access/';

async function downloadFile(url, filePath) {
  const response = await axios({
    url,
    method: 'GET',
    responseType: 'stream',
  });
  return new Promise((resolve, reject) => {
    const writer = fs.createWriteStream(filePath);
    response.data.pipe(writer);
    writer.on('finish', resolve);
    writer.on('error', reject);
  });
}

async function main() {
  try {
    const { data } = await axios.get(baseURL);
    const $ = cheerio.load(data);

    const dir = './NOAA_files';
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }

    const links = $('a[href$=".csv"]');
    for (let i = 0; i < links.length; i++) {
      const fileURL = $(links[i]).attr('href');
      const fullURL = baseURL + fileURL;
      const filePath = path.join(dir, fileURL);

      if (fs.existsSync(filePath)) {
        // console.log(`Skipping ${filePath} - already exists`);
        // eslint-disable-next-line no-continue
        continue;
      }

      console.log(`Downloading ${fullURL} to ${filePath}`);
      try {
        // eslint-disable-next-line no-await-in-loop
        await downloadFile(fullURL, filePath);
      } catch (error) {
        console.error(`Error downloading ${fullURL}:`, error.message);
      }
    }

    console.log('Download completed.');
  } catch (error) {
    console.error('Error fetching page:', error.message);
  }
}

main();
