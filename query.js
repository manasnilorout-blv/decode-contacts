const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');

function queryDatabase(dbPath, query) {
  const db = new sqlite3.Database(dbPath, sqlite3.OPEN_READONLY, (err) => {
    if (err) {
      console.error('Error opening database:', err);
      return;
    }
  });

  db.all(query, [], (err, rows) => {
    if (err) {
      console.error('Query error:', err);
      return;
    }
    
    console.log(`\nQuery: ${query}`);
    console.log(`Results (${rows.length} rows):`);
    console.log('─'.repeat(80));
    
    if (rows.length > 0) {
      console.table(rows);
    } else {
      console.log('No results found.');
    }
  });

  db.close();
}

const outputDir = path.join(__dirname, 'output');
const dbFiles = fs.readdirSync(outputDir).filter(f => f.endsWith('.db'));

if (dbFiles.length === 0) {
  console.log('No database files found in output directory.');
  process.exit(1);
}

const latestDb = dbFiles.sort().pop();
const dbPath = path.join(outputDir, latestDb);

console.log(`Using database: ${latestDb}`);
console.log('='.repeat(80));

queryDatabase(dbPath, `
  SELECT COUNT(*) as total_contacts FROM deduplicated_contacts
`);

setTimeout(() => {
  queryDatabase(dbPath, `
    SELECT 
      COUNT(CASE WHEN email IS NOT NULL AND email != '' THEN 1 END) as with_email,
      COUNT(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 END) as with_phone,
      COUNT(CASE WHEN email IS NOT NULL AND email != '' AND phone IS NOT NULL AND phone != '' THEN 1 END) as with_both
    FROM deduplicated_contacts
  `);
}, 500);

setTimeout(() => {
  queryDatabase(dbPath, `
    SELECT * FROM deduplicated_contacts 
    WHERE email IS NOT NULL AND phone IS NOT NULL 
    LIMIT 10
  `);
}, 1000);

setTimeout(() => {
  queryDatabase(dbPath, `
    SELECT * FROM deduplicated_contacts 
    WHERE linkedin_name != '' 
    LIMIT 10
  `);
}, 1500);