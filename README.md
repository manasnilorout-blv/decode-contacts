# Contact Deduplication Application

A Node.js application that deduplicates contacts from multiple CSV sources and exports them to a centralized CSV file and SQLite database.

## Features

- **Multiple CSV Source Support**:
  - Email inbox/sent CSV files (Outlook format)
  - LinkedIn connections export
  - Phone book contacts
  
- **Smart Deduplication**:
  - Email-based matching
  - Phone number normalization and matching
  - Fuzzy name matching with configurable threshold
  - Priority-based source selection (LinkedIn > Phonebook > Email)

- **Dual Export Format**:
  - CSV file with standardized columns
  - SQLite database for advanced querying

## Installation

```bash
npm install
```

## Usage

### Run Deduplication

```bash
node dedupe.js
```

This will:
1. Process all CSV files in the `/contacts` directory
2. Deduplicate contacts based on email, phone, and fuzzy name matching
3. Export results to `/output` directory as both CSV and SQLite database

### Query Results

```bash
node query.js
```

This will display statistics and sample data from the SQLite database.

## Input CSV Formats

### Email CSV (email_inbox.csv, email_sent.csv)
- Columns: `From: (Name)`, `From: (Address)`, `To: (Name)`, `To: (Address)`, `CC: (Name)`, `CC: (Address)`

### LinkedIn CSV (linkedin_connections.csv)
- Columns: `First Name`, `Last Name`, `Email Address`, `Company`, `Position`

### Phonebook CSV (phonebook.csv)
- Columns: `Full Name`, `E-mail Address`, `Mobile Phone`, `Primary Phone`, etc.

## Output Format

The deduplicated CSV contains the following columns:
- **LinkedIn Name**: Name from LinkedIn connections
- **Phone Book Name**: Name from phonebook
- **Email Name**: Name from email communications
- **email**: Deduplicated email address
- **phone**: Normalized phone number

## Deduplication Logic

1. **Email Extraction**: Extracts valid email addresses from various formats
2. **Phone Normalization**: Cleans and standardizes phone numbers (handles Indian +91 format)
3. **Name Normalization**: Removes special characters and standardizes formatting
4. **Fuzzy Matching**: Uses FuzzySet.js for similar name detection (75% threshold)
5. **Priority Selection**: LinkedIn > Phonebook > Email for name conflicts

## Project Structure

```
contact-dedupe/
├── contacts/           # Input CSV files
├── output/            # Generated deduplicated files
├── dedupe.js          # Main deduplication application
├── query.js           # Database query utility
└── package.json       # Project dependencies
```

## Statistics from Latest Run

- Total contacts processed: 162,115
- Unique contacts after deduplication: 13,771
- Contacts with email: 8,681
- Contacts with phone: 2,757
- Contacts with both email and phone: 43

## Dependencies

- `csv-parse`: CSV parsing
- `csv-stringify`: CSV generation
- `sqlite3`: SQLite database operations
- `fuzzyset.js`: Fuzzy string matching