const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const { parse } = require('csv-parse');
const { stringify } = require('csv-stringify');

const app = express();
const PORT = 3000;

const upload = multer({ dest: 'uploads/' });

app.use(express.static('public'));

function extractEmail(address) {
  if (!address) return null;
  
  const emailRegex = /([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/;
  const match = address.match(emailRegex);
  if (match) {
    return match[1].toLowerCase();
  }
  
  if (address.includes('@')) {
    return address.toLowerCase();
  }
  
  return null;
}

function processCSV(inputPath, outputPath, callback) {
  const contacts = new Map();
  
  const input = fs.createReadStream(inputPath);
  const parser = parse({
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true
  });

  parser.on('readable', function() {
    let record;
    while ((record = parser.read()) !== null) {
      const fromName = record['From: (Name)'];
      const fromAddress = record['From: (Address)'];
      if (fromName && fromAddress) {
        const email = extractEmail(fromAddress);
        if (email && !contacts.has(email)) {
          contacts.set(email, {
            name: fromName.trim(),
            email: email
          });
        }
      }
      
      const toNames = record['To: (Name)'] ? record['To: (Name)'].split(';') : [];
      const toAddresses = record['To: (Address)'] ? record['To: (Address)'].split(';') : [];
      
      for (let i = 0; i < Math.max(toNames.length, toAddresses.length); i++) {
        const name = toNames[i] ? toNames[i].trim() : '';
        const address = toAddresses[i] ? toAddresses[i].trim() : '';
        
        if (name && address) {
          const email = extractEmail(address);
          if (email && !contacts.has(email)) {
            contacts.set(email, {
              name: name,
              email: email
            });
          }
        }
      }
      
      const ccNames = record['CC: (Name)'] ? record['CC: (Name)'].split(';') : [];
      const ccAddresses = record['CC: (Address)'] ? record['CC: (Address)'].split(';') : [];
      
      for (let i = 0; i < Math.max(ccNames.length, ccAddresses.length); i++) {
        const name = ccNames[i] ? ccNames[i].trim() : '';
        const address = ccAddresses[i] ? ccAddresses[i].trim() : '';
        
        if (name && address) {
          const email = extractEmail(address);
          if (email && !contacts.has(email)) {
            contacts.set(email, {
              name: name,
              email: email
            });
          }
        }
      }
    }
  });

  parser.on('error', function(err) {
    console.error('Error:', err.message);
    callback(err);
  });

  parser.on('end', function() {
    const deduplicatedContacts = Array.from(contacts.values());
    
    const stringifier = stringify({
      header: true,
      columns: ['name', 'email']
    });
    
    const output = fs.createWriteStream(outputPath);
    stringifier.pipe(output);
    
    deduplicatedContacts.forEach(contact => {
      stringifier.write(contact);
    });
    
    stringifier.end();
    
    stringifier.on('finish', () => {
      callback(null, deduplicatedContacts.length);
    });
  });

  input.pipe(parser);
}

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.post('/upload', upload.single('csvfile'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'No file uploaded' });
  }

  const outputPath = path.join(__dirname, 'outputs', `deduplicated_${Date.now()}.csv`);
  
  if (!fs.existsSync(path.join(__dirname, 'outputs'))) {
    fs.mkdirSync(path.join(__dirname, 'outputs'));
  }

  processCSV(req.file.path, outputPath, (err, count) => {
    fs.unlinkSync(req.file.path);
    
    if (err) {
      return res.status(500).json({ error: 'Error processing CSV file' });
    }

    res.json({
      success: true,
      totalContacts: count,
      downloadPath: `/download/${path.basename(outputPath)}`
    });
  });
});

app.get('/download/:filename', (req, res) => {
  const filepath = path.join(__dirname, 'outputs', req.params.filename);
  
  if (!fs.existsSync(filepath)) {
    return res.status(404).json({ error: 'File not found' });
  }

  res.download(filepath, 'deduplicated_contacts.csv', (err) => {
    if (!err) {
      fs.unlinkSync(filepath);
    }
  });
});

app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});