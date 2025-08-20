const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { stringify } = require('csv-stringify');
const sqlite3 = require('sqlite3').verbose();
const FuzzySet = require('fuzzyset.js');

class ContactDeduplicator {
  constructor() {
    this.db = null;
    this.fuzzyNames = null;
    this.nameToId = new Map();
  }

  async initialize() {
    return new Promise((resolve, reject) => {
      this.db = new sqlite3.Database(':memory:', (err) => {
        if (err) {
          reject(err);
        } else {
          this.createTables().then(resolve).catch(reject);
        }
      });
    });
  }

  async createTables() {
    return new Promise((resolve, reject) => {
      const sql = `
        CREATE TABLE contacts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT,
          original_name TEXT,
          normalized_name TEXT,
          email TEXT,
          phone TEXT,
          linkedin_name TEXT,
          phonebook_name TEXT,
          email_name TEXT,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX idx_email ON contacts(email);
        CREATE INDEX idx_phone ON contacts(phone);
        CREATE INDEX idx_normalized_name ON contacts(normalized_name);
      `;
      
      this.db.exec(sql, (err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  extractEmail(address) {
    if (!address) return null;
    
    const emailRegex = /([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/;
    const match = address.match(emailRegex);
    if (match) {
      return match[1].toLowerCase().trim();
    }
    
    if (address.includes('@')) {
      return address.toLowerCase().trim();
    }
    
    return null;
  }

  extractPhone(phoneStr) {
    if (!phoneStr) return null;
    
    const cleaned = phoneStr.toString().replace(/\D/g, '');
    
    if (cleaned.length >= 10) {
      if (cleaned.startsWith('91') && cleaned.length >= 12) {
        return cleaned.substring(0, 12);
      }
      return cleaned.substring(cleaned.length - 10);
    }
    
    return null;
  }

  normalizeName(name) {
    if (!name) return null;
    
    return name
      .replace(/[^a-zA-Z\s]/g, ' ')
      .toLowerCase()
      .split(/\s+/)
      .filter(part => part.length > 0)
      .join(' ')
      .trim();
  }

  async processEmailCSV(filePath, source) {
    return new Promise((resolve, reject) => {
      const contacts = [];
      const parser = parse({
        columns: true,
        skip_empty_lines: true,
        relax_column_count: true
      });

      parser.on('readable', () => {
        let record;
        while ((record = parser.read()) !== null) {
          const processNameAndAddress = (name, address, type) => {
            if (name && address) {
              const email = this.extractEmail(address);
              if (email) {
                contacts.push({
                  source,
                  original_name: name.trim(),
                  normalized_name: this.normalizeName(name),
                  email,
                  phone: null,
                  email_name: name.trim()
                });
              }
            }
          };

          processNameAndAddress(record['From: (Name)'], record['From: (Address)'], 'from');
          
          const toNames = record['To: (Name)'] ? record['To: (Name)'].split(';') : [];
          const toAddresses = record['To: (Address)'] ? record['To: (Address)'].split(';') : [];
          for (let i = 0; i < Math.max(toNames.length, toAddresses.length); i++) {
            processNameAndAddress(
              toNames[i] ? toNames[i].trim() : '',
              toAddresses[i] ? toAddresses[i].trim() : '',
              'to'
            );
          }
          
          const ccNames = record['CC: (Name)'] ? record['CC: (Name)'].split(';') : [];
          const ccAddresses = record['CC: (Address)'] ? record['CC: (Address)'].split(';') : [];
          for (let i = 0; i < Math.max(ccNames.length, ccAddresses.length); i++) {
            processNameAndAddress(
              ccNames[i] ? ccNames[i].trim() : '',
              ccAddresses[i] ? ccAddresses[i].trim() : '',
              'cc'
            );
          }
        }
      });

      parser.on('error', reject);
      parser.on('end', () => resolve(contacts));

      fs.createReadStream(filePath).pipe(parser);
    });
  }

  async processLinkedInCSV(filePath) {
    return new Promise((resolve, reject) => {
      const contacts = [];
      let headerFound = false;
      
      const parser = parse({
        skip_empty_lines: true,
        relax_column_count: true
      });

      parser.on('readable', () => {
        let record;
        while ((record = parser.read()) !== null) {
          if (!headerFound) {
            if (record[0] === 'First Name' && record[1] === 'Last Name') {
              headerFound = true;
            }
            continue;
          }
          
          const firstName = record[0] || '';
          const lastName = record[1] || '';
          const email = record[3] ? this.extractEmail(record[3]) : null;
          const fullName = `${firstName} ${lastName}`.trim();
          
          if (fullName) {
            contacts.push({
              source: 'linkedin',
              original_name: fullName,
              normalized_name: this.normalizeName(fullName),
              email,
              phone: null,
              linkedin_name: fullName
            });
          }
        }
      });

      parser.on('error', reject);
      parser.on('end', () => resolve(contacts));

      fs.createReadStream(filePath).pipe(parser);
    });
  }

  async processPhonebookCSV(filePath) {
    return new Promise((resolve, reject) => {
      const contacts = [];
      
      const parser = parse({
        columns: true,
        skip_empty_lines: true,
        relax_column_count: true
      });

      parser.on('readable', () => {
        let record;
        while ((record = parser.read()) !== null) {
          const fullName = record['Full Name'] || '';
          const email = this.extractEmail(record['E-mail Address'] || record['E-mail 2 Address'] || record['E-mail 3 Address']);
          const phone = this.extractPhone(
            record['Mobile Phone'] || 
            record['Primary Phone'] || 
            record['Home Phone'] || 
            record['Business Phone'] ||
            record['Other Phone']
          );
          
          if (fullName && (email || phone)) {
            contacts.push({
              source: 'phonebook',
              original_name: fullName.trim(),
              normalized_name: this.normalizeName(fullName),
              email,
              phone,
              phonebook_name: fullName.trim()
            });
          }
        }
      });

      parser.on('error', reject);
      parser.on('end', () => resolve(contacts));

      fs.createReadStream(filePath).pipe(parser);
    });
  }

  async insertContacts(contacts) {
    return new Promise((resolve, reject) => {
      const stmt = this.db.prepare(`
        INSERT INTO contacts (source, original_name, normalized_name, email, phone, linkedin_name, phonebook_name, email_name)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `);

      let inserted = 0;
      contacts.forEach(contact => {
        stmt.run(
          contact.source,
          contact.original_name,
          contact.normalized_name,
          contact.email,
          contact.phone,
          contact.linkedin_name || null,
          contact.phonebook_name || null,
          contact.email_name || null,
          (err) => {
            if (err) console.error('Insert error:', err);
            else inserted++;
          }
        );
      });

      stmt.finalize((err) => {
        if (err) reject(err);
        else resolve(inserted);
      });
    });
  }

  async buildFuzzyMatcher() {
    return new Promise((resolve, reject) => {
      this.db.all(
        'SELECT DISTINCT normalized_name FROM contacts WHERE normalized_name IS NOT NULL',
        (err, rows) => {
          if (err) {
            reject(err);
          } else {
            const names = rows.map(r => r.normalized_name);
            if (names.length > 0) {
              this.fuzzyNames = FuzzySet(names);
              names.forEach(name => {
                this.nameToId.set(name, name);
              });
            }
            resolve();
          }
        }
      );
    });
  }

  findSimilarName(name, threshold = 0.75) {
    if (!this.fuzzyNames || !name) return null;
    
    const normalized = this.normalizeName(name);
    if (!normalized) return null;
    
    const matches = this.fuzzyNames.get(normalized, null, threshold);
    if (matches && matches.length > 0) {
      return matches[0][1];
    }
    
    return null;
  }

  async performFuzzyMatching() {
    return new Promise((resolve, reject) => {
      // Get all contacts with normalized names for fuzzy matching
      const sql = `
        SELECT id, source, original_name, normalized_name, email, phone
        FROM contacts 
        WHERE normalized_name IS NOT NULL
        ORDER BY source, normalized_name
      `;
      
      this.db.all(sql, async (err, rows) => {
        if (err) {
          reject(err);
          return;
        }

        // Create groups based on fuzzy name matching
        const groups = new Map();
        const processedIds = new Set();
        
        for (let i = 0; i < rows.length; i++) {
          if (processedIds.has(rows[i].id)) continue;
          
          const group = [rows[i]];
          processedIds.add(rows[i].id);
          
          // Find similar names
          for (let j = i + 1; j < rows.length; j++) {
            if (processedIds.has(rows[j].id)) continue;
            
            if (this.areNamesSimilar(rows[i].normalized_name, rows[j].normalized_name)) {
              group.push(rows[j]);
              processedIds.add(rows[j].id);
            }
          }
          
          groups.set(`group_${i}`, group);
        }
        
        resolve(groups);
      });
    });
  }

  areNamesSimilar(name1, name2, threshold = 0.7) {
    if (!name1 || !name2) return false;
    
    // Direct match
    if (name1 === name2) return true;
    
    // Split names into words and check for substantial overlap
    const words1 = name1.split(/\s+/).filter(w => w.length > 2);
    const words2 = name2.split(/\s+/).filter(w => w.length > 2);
    
    if (words1.length === 0 || words2.length === 0) return false;
    
    // Count matching words
    let matchingWords = 0;
    for (const word1 of words1) {
      for (const word2 of words2) {
        if (this.getLevenshteinSimilarity(word1, word2) >= 0.8) {
          matchingWords++;
          break;
        }
      }
    }
    
    // Calculate similarity based on matching words
    const similarity = (2 * matchingWords) / (words1.length + words2.length);
    return similarity >= threshold;
  }

  getLevenshteinSimilarity(str1, str2) {
    const longer = str1.length > str2.length ? str1 : str2;
    const shorter = str1.length > str2.length ? str2 : str1;
    
    if (longer.length === 0) return 1.0;
    
    const distance = this.levenshteinDistance(longer, shorter);
    return (longer.length - distance) / longer.length;
  }

  levenshteinDistance(str1, str2) {
    const matrix = Array(str2.length + 1).fill().map(() => Array(str1.length + 1).fill(0));
    
    for (let i = 0; i <= str1.length; i++) matrix[0][i] = i;
    for (let j = 0; j <= str2.length; j++) matrix[j][0] = j;
    
    for (let j = 1; j <= str2.length; j++) {
      for (let i = 1; i <= str1.length; i++) {
        const cost = str1[i - 1] === str2[j - 1] ? 0 : 1;
        matrix[j][i] = Math.min(
          matrix[j - 1][i] + 1,
          matrix[j][i - 1] + 1,
          matrix[j - 1][i - 1] + cost
        );
      }
    }
    
    return matrix[str2.length][str1.length];
  }

  async deduplicateContacts() {
    return new Promise(async (resolve, reject) => {
      try {
        const sql = `
          WITH DeduplicatedContacts AS (
            -- Contacts with email addresses (group by email)
            SELECT 
              LOWER(TRIM(email)) as email_normalized,
              TRIM(phone) as phone_normalized,
              MAX(CASE WHEN source = 'linkedin' THEN original_name END) as linkedin_name,
              MAX(CASE WHEN source = 'phonebook' THEN original_name END) as phonebook_name,
              MAX(CASE WHEN source IN ('email_inbox', 'email_sent') THEN original_name END) as email_name
            FROM contacts
            WHERE email IS NOT NULL AND TRIM(email) != ''
            GROUP BY LOWER(TRIM(email))
            
            UNION ALL
            
            -- Contacts with only phone (no email)
            SELECT 
              NULL as email_normalized,
              TRIM(phone) as phone_normalized,
              MAX(CASE WHEN source = 'linkedin' THEN original_name END) as linkedin_name,
              MAX(CASE WHEN source = 'phonebook' THEN original_name END) as phonebook_name,
              MAX(CASE WHEN source IN ('email_inbox', 'email_sent') THEN original_name END) as email_name
            FROM contacts
            WHERE (email IS NULL OR TRIM(email) = '') 
              AND phone IS NOT NULL AND TRIM(phone) != ''
            GROUP BY TRIM(phone)
            
            UNION ALL
            
            -- LinkedIn contacts without email or phone (keep them all for now)
            SELECT 
              NULL as email_normalized,
              NULL as phone_normalized,
              original_name as linkedin_name,
              NULL as phonebook_name,
              NULL as email_name
            FROM contacts
            WHERE source = 'linkedin' 
              AND (email IS NULL OR TRIM(email) = '')
              AND (phone IS NULL OR TRIM(phone) = '')
              
            UNION ALL
            
            -- Phonebook contacts without email or phone
            SELECT 
              NULL as email_normalized,
              NULL as phone_normalized,
              NULL as linkedin_name,
              original_name as phonebook_name,
              NULL as email_name
            FROM contacts
            WHERE source = 'phonebook' 
              AND (email IS NULL OR TRIM(email) = '')
              AND (phone IS NULL OR TRIM(phone) = '')
          )
          SELECT 
            COALESCE(linkedin_name, 'N/A') as linkedin_name,
            COALESCE(phonebook_name, 'N/A') as phonebook_name,
            COALESCE(email_name, 'N/A') as email_name,
            email_normalized as email,
            phone_normalized as phone
          FROM DeduplicatedContacts
        `;

        this.db.all(sql, async (err, initialResults) => {
          if (err) {
            reject(err);
            return;
          }

          console.log(`Initial results: ${initialResults.length} contacts`);
          
          // Apply focused fuzzy matching only to contacts without email/phone
          const mergedResults = await this.mergeSimilarContactsFocused(initialResults);
          
          console.log(`After fuzzy matching: ${mergedResults.length} contacts`);
          
          // Sort results
          mergedResults.sort((a, b) => {
            const aScore = a.linkedin_name !== 'N/A' ? 1 : (a.phonebook_name !== 'N/A' ? 2 : 3);
            const bScore = b.linkedin_name !== 'N/A' ? 1 : (b.phonebook_name !== 'N/A' ? 2 : 3);
            
            if (aScore !== bScore) return aScore - bScore;
            
            const aName = a.linkedin_name !== 'N/A' ? a.linkedin_name : (a.phonebook_name !== 'N/A' ? a.phonebook_name : a.email_name);
            const bName = b.linkedin_name !== 'N/A' ? b.linkedin_name : (b.phonebook_name !== 'N/A' ? b.phonebook_name : b.email_name);
            
            return (aName || '').localeCompare(bName || '');
          });
          
          resolve(mergedResults);
        });
        
      } catch (error) {
        reject(error);
      }
    });
  }

  async mergeSimilarContacts(contacts) {
    const merged = [];
    const processed = new Set();
    
    for (let i = 0; i < contacts.length; i++) {
      if (processed.has(i)) continue;
      
      const current = { ...contacts[i] };
      processed.add(i);
      
      // Find similar contacts to merge
      for (let j = i + 1; j < contacts.length; j++) {
        if (processed.has(j)) continue;
        
        const candidate = contacts[j];
        
        // Skip if they have different emails or phones (already properly grouped)
        if (current.email && candidate.email && current.email !== candidate.email) continue;
        if (current.phone && candidate.phone && current.phone !== candidate.phone) continue;
        
        // Check if names are similar
        const currentName = this.getNameForComparison(current);
        const candidateName = this.getNameForComparison(candidate);
        
        if (currentName && candidateName && this.areNamesSimilar(
          this.normalizeName(currentName), 
          this.normalizeName(candidateName)
        )) {
          // Merge the contacts
          if (candidate.linkedin_name !== 'N/A') current.linkedin_name = candidate.linkedin_name;
          if (candidate.phonebook_name !== 'N/A') current.phonebook_name = candidate.phonebook_name;
          if (candidate.email_name !== 'N/A') current.email_name = candidate.email_name;
          if (!current.email && candidate.email) current.email = candidate.email;
          if (!current.phone && candidate.phone) current.phone = candidate.phone;
          
          processed.add(j);
        }
      }
      
      merged.push(current);
    }
    
    return merged;
  }

  getNameForComparison(contact) {
    if (contact.linkedin_name !== 'N/A') return contact.linkedin_name;
    if (contact.phonebook_name !== 'N/A') return contact.phonebook_name;
    if (contact.email_name !== 'N/A') return contact.email_name;
    return null;
  }

  async mergeSimilarContactsFocused(contacts) {
    console.log(`Processing ${contacts.length} contacts for targeted fuzzy matching...`);
    
    // Create name index for faster lookups
    const nameIndex = new Map();
    contacts.forEach((contact, index) => {
      const name = this.getNameForComparison(contact);
      if (name) {
        const normalized = this.normalizeName(name);
        const words = normalized.split(/\s+/).filter(w => w.length > 2);
        
        // Index by each significant word
        words.forEach(word => {
          if (!nameIndex.has(word)) {
            nameIndex.set(word, []);
          }
          nameIndex.get(word).push({ index, contact, name, normalized });
        });
      }
    });
    
    const merged = [];
    const processed = new Set();
    let mergeCount = 0;
    
    // Process contacts and find matches using the index
    for (let i = 0; i < contacts.length; i++) {
      if (processed.has(i)) continue;
      
      const current = { ...contacts[i] };
      processed.add(i);
      
      const currentName = this.getNameForComparison(current);
      if (!currentName) {
        merged.push(current);
        continue;
      }
      
      const currentNormalized = this.normalizeName(currentName);
      const currentWords = currentNormalized.split(/\s+/).filter(w => w.length > 2);
      
      // Find candidates using the index
      const candidates = new Set();
      currentWords.forEach(word => {
        if (nameIndex.has(word)) {
          nameIndex.get(word).forEach(entry => {
            if (entry.index !== i && !processed.has(entry.index)) {
              candidates.add(entry.index);
            }
          });
        }
      });
      
      // Check candidates for similarity
      for (const j of candidates) {
        if (processed.has(j)) continue;
        
        const candidate = contacts[j];
        
        // Skip if they have conflicting emails or phones
        if (current.email && candidate.email && current.email !== candidate.email) continue;
        if (current.phone && candidate.phone && current.phone !== candidate.phone) continue;
        
        const candidateName = this.getNameForComparison(candidate);
        
        if (candidateName && this.areNamesSimilar(
          currentNormalized,
          this.normalizeName(candidateName),
          0.7
        )) {
          console.log(`Merging: "${currentName}" with "${candidateName}"`);
          
          // Merge the contacts - keep all source information
          if (candidate.linkedin_name !== 'N/A') current.linkedin_name = candidate.linkedin_name;
          if (candidate.phonebook_name !== 'N/A') current.phonebook_name = candidate.phonebook_name;
          if (candidate.email_name !== 'N/A') current.email_name = candidate.email_name;
          
          // Merge contact information
          if (!current.email && candidate.email) current.email = candidate.email;
          if (!current.phone && candidate.phone) current.phone = candidate.phone;
          
          processed.add(j);
          mergeCount++;
        }
      }
      
      merged.push(current);
    }
    
    console.log(`Merged ${mergeCount} similar contacts`);
    console.log(`Final count: ${merged.length} (from ${contacts.length} initial contacts)`);
    
    // Verify no contacts were lost
    if (merged.length + mergeCount !== contacts.length) {
      console.warn(`Warning: Contact count mismatch! Input: ${contacts.length}, Output: ${merged.length}, Merged: ${mergeCount}`);
    }
    
    return merged;
  }

  async exportToCSV(deduplicatedContacts, outputPath) {
    return new Promise((resolve, reject) => {
      const stringifier = stringify({
        header: true,
        columns: ['LinkedIn Name', 'Phone Book Name', 'Email Name', 'email', 'phone']
      });

      const output = fs.createWriteStream(outputPath);
      stringifier.pipe(output);

      deduplicatedContacts.forEach(contact => {
        stringifier.write({
          'LinkedIn Name': contact.linkedin_name || '',
          'Phone Book Name': contact.phonebook_name || '',
          'Email Name': contact.email_name || '',
          'email': contact.email || '',
          'phone': contact.phone || ''
        });
      });

      stringifier.end();
      
      stringifier.on('finish', () => resolve(deduplicatedContacts.length));
      stringifier.on('error', reject);
    });
  }

  async exportToSQLite(deduplicatedContacts, dbPath) {
    return new Promise((resolve, reject) => {
      const exportDb = new sqlite3.Database(dbPath, (err) => {
        if (err) {
          reject(err);
          return;
        }

        const sql = `
          CREATE TABLE IF NOT EXISTS deduplicated_contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            linkedin_name TEXT,
            phonebook_name TEXT,
            email_name TEXT,
            email TEXT UNIQUE,
            phone TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
          );
        `;

        exportDb.run(sql, (err) => {
          if (err) {
            reject(err);
            return;
          }

          const stmt = exportDb.prepare(`
            INSERT OR REPLACE INTO deduplicated_contacts 
            (linkedin_name, phonebook_name, email_name, email, phone)
            VALUES (?, ?, ?, ?, ?)
          `);

          deduplicatedContacts.forEach(contact => {
            stmt.run(
              contact.linkedin_name || '',
              contact.phonebook_name || '',
              contact.email_name || '',
              contact.email || '',
              contact.phone || ''
            );
          });

          stmt.finalize((err) => {
            if (err) {
              reject(err);
            } else {
              exportDb.close((closeErr) => {
                if (closeErr) reject(closeErr);
                else resolve(deduplicatedContacts.length);
              });
            }
          });
        });
      });
    });
  }

  async close() {
    return new Promise((resolve, reject) => {
      if (this.db) {
        this.db.close((err) => {
          if (err) reject(err);
          else resolve();
        });
      } else {
        resolve();
      }
    });
  }
}

async function main() {
  const deduplicator = new ContactDeduplicator();
  
  try {
    console.log('Initializing database...');
    await deduplicator.initialize();

    console.log('\nProcessing CSV files...');
    
    const emailInboxPath = path.join(__dirname, 'contacts', 'email_inbox.csv');
    if (fs.existsSync(emailInboxPath)) {
      console.log('Processing email_inbox.csv...');
      const contacts = await deduplicator.processEmailCSV(emailInboxPath, 'email_inbox');
      await deduplicator.insertContacts(contacts);
      console.log(`  - Processed ${contacts.length} contacts from email_inbox.csv`);
    }

    const emailSentPath = path.join(__dirname, 'contacts', 'email_sent.csv');
    if (fs.existsSync(emailSentPath)) {
      console.log('Processing email_sent.csv...');
      const contacts = await deduplicator.processEmailCSV(emailSentPath, 'email_sent');
      await deduplicator.insertContacts(contacts);
      console.log(`  - Processed ${contacts.length} contacts from email_sent.csv`);
    }

    const linkedinPath = path.join(__dirname, 'contacts', 'linkedin_connections.csv');
    if (fs.existsSync(linkedinPath)) {
      console.log('Processing linkedin_connections.csv...');
      const contacts = await deduplicator.processLinkedInCSV(linkedinPath);
      await deduplicator.insertContacts(contacts);
      console.log(`  - Processed ${contacts.length} contacts from linkedin_connections.csv`);
    }

    const phonebookPath = path.join(__dirname, 'contacts', 'phonebook.csv');
    if (fs.existsSync(phonebookPath)) {
      console.log('Processing phonebook.csv...');
      const contacts = await deduplicator.processPhonebookCSV(phonebookPath);
      await deduplicator.insertContacts(contacts);
      console.log(`  - Processed ${contacts.length} contacts from phonebook.csv`);
    }

    console.log('\nBuilding fuzzy matcher for name deduplication...');
    await deduplicator.buildFuzzyMatcher();

    console.log('Deduplicating contacts...');
    const deduplicatedContacts = await deduplicator.deduplicateContacts();
    console.log(`Found ${deduplicatedContacts.length} unique contacts after deduplication`);

    const outputDir = path.join(__dirname, 'output');
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir);
    }

    const csvOutputPath = path.join(outputDir, `deduplicated_contacts_${Date.now()}.csv`);
    await deduplicator.exportToCSV(deduplicatedContacts, csvOutputPath);
    console.log(`\nExported to CSV: ${csvOutputPath}`);

    const dbOutputPath = path.join(outputDir, `deduplicated_contacts_${Date.now()}.db`);
    await deduplicator.exportToSQLite(deduplicatedContacts, dbOutputPath);
    console.log(`Exported to SQLite: ${dbOutputPath}`);

    console.log('\n✓ Deduplication complete!');
    console.log(`Total unique contacts: ${deduplicatedContacts.length}`);
    
    const stats = {
      withEmail: deduplicatedContacts.filter(c => c.email).length,
      withPhone: deduplicatedContacts.filter(c => c.phone).length,
      withBoth: deduplicatedContacts.filter(c => c.email && c.phone).length
    };
    
    console.log(`\nStatistics:`);
    console.log(`  - Contacts with email: ${stats.withEmail}`);
    console.log(`  - Contacts with phone: ${stats.withPhone}`);
    console.log(`  - Contacts with both: ${stats.withBoth}`);

    await deduplicator.close();
  } catch (error) {
    console.error('Error:', error);
    await deduplicator.close();
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = ContactDeduplicator;