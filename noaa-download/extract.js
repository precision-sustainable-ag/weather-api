const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const dir = './NOAA_files'; // Directory where CSV files are stored

// Array to hold the extracted data
const results = [];

// Function to process a single CSV file
function processFile(filePath) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => {
        // Extract the required fields
        if (data.LATITUDE && data.LONGITUDE && data['ANN-PRCP-NORMAL']) {
          results.push({
            lat: +data.LATITUDE,
            lon: +data.LONGITUDE,
            rain: +data['ANN-PRCP-NORMAL'],
          });
        }
      })
      .on('end', resolve)
      .on('error', reject);
  });
}

// Main function to process all CSV files in the directory
async function main() {
  try {
    const files = fs.readdirSync(dir);

    // Process each file sequentially
    // eslint-disable-next-line no-restricted-syntax
    for (const file of files) {
      if (path.extname(file) === '.csv') {
        const filePath = path.join(dir, file);
        console.log(`Processing ${filePath}`);
        // eslint-disable-next-line no-await-in-loop
        await processFile(filePath);
      }
    }

    // Write the extracted data to a JSON file
    fs.writeFileSync('extracted_data.json', JSON.stringify(results, null, 2));
    console.log('Extraction completed. Data saved to extracted_data.json');
  } catch (error) {
    console.error('Error processing files:', error);
  }
}

main();
