import fs from 'node:fs';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';

const csvPath = path.resolve(process.argv[2] ?? 'sample_ad_data.csv');
const dbPath = path.resolve(process.argv[3] ?? 'sample_ad_data.db');

if (!fs.existsSync(csvPath)) {
  console.error(`CSV file not found: ${csvPath}`);
  process.exit(1);
}

const db = new DatabaseSync(dbPath);

db.exec(`
  PRAGMA journal_mode = WAL;
  PRAGMA synchronous = NORMAL;

  DROP TABLE IF EXISTS ad_data;

  CREATE TABLE ad_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id TEXT NOT NULL,
    date TEXT NOT NULL,
    impressions INTEGER NOT NULL,
    clicks INTEGER NOT NULL,
    spend REAL NOT NULL,
    conversions INTEGER NOT NULL
  );
`);

const insertStmt = db.prepare(`
  INSERT INTO ad_data (
    campaign_id,
    date,
    impressions,
    clicks,
    spend,
    conversions
  ) VALUES (?, ?, ?, ?, ?, ?)
`);

const content = fs.readFileSync(csvPath, 'utf8').trim();
const lines = content.split(/\r?\n/);

if (lines.length < 2) {
  console.error('CSV has no data rows');
  process.exit(1);
}

const header = lines[0].split(',').map((v) => v.trim());
const expected = ['campaign_id', 'date', 'impressions', 'clicks', 'spend', 'conversions'];

if (JSON.stringify(header) !== JSON.stringify(expected)) {
  console.error(`Unexpected CSV header: ${header.join(',')}`);
  process.exit(1);
}

let inserted = 0;
db.exec('BEGIN');
try {
  for (const line of lines.slice(1)) {
    if (!line.trim()) continue;
    const cols = line.split(',').map((v) => v.trim());
    if (cols.length !== 6) continue;

    insertStmt.run(
      cols[0],
      cols[1],
      Number.parseInt(cols[2], 10),
      Number.parseInt(cols[3], 10),
      Number.parseFloat(cols[4]),
      Number.parseInt(cols[5], 10)
    );
    inserted++;
  }
  db.exec('COMMIT');
} catch (err) {
  db.exec('ROLLBACK');
  throw err;
}

const countRow = db.prepare('SELECT COUNT(*) AS total FROM ad_data').get();
console.log(`Database: ${dbPath}`);
console.log(`Inserted rows: ${inserted}`);
console.log(`Rows in table: ${countRow.total}`);

db.close();
