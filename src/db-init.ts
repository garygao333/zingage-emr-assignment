import { Client } from 'pg';
import fs from 'fs';
import path from 'path';

async function runSQLFile(connectionString: string, sqlFile: string) {
  const client = new Client({ connectionString });

  try {
    await client.connect();
    const sql = fs.readFileSync(sqlFile, 'utf8');

    // Split by semicolon but handle functions/procedures properly
    const statements = sql
      .split(/;(?=\s*(?:--|$|CREATE|DROP|INSERT|UPDATE|DELETE|ALTER|SELECT|WITH))/gi)
      .map(s => s.trim())
      .filter(s => s.length > 0 && !s.startsWith('--'));

    console.log(`Running ${statements.length} statements from ${path.basename(sqlFile)}...`);

    for (const statement of statements) {
      if (statement.trim()) {
        await client.query(statement);
      }
    }

    console.log(`✓ Successfully executed ${sqlFile}`);
  } catch (error) {
    console.error(`✗ Error executing ${sqlFile}:`, error);
    throw error;
  } finally {
    await client.end();
  }
}

async function main() {
  const DATABASE_URL = process.env.DATABASE_URL;

  if (!DATABASE_URL) {
    console.error('DATABASE_URL not set in environment');
    process.exit(1);
  }

  const command = process.argv[2];

  try {
    switch (command) {
      case 'init':
        await runSQLFile(DATABASE_URL, 'sql/ddl_init.sql');
        break;
      case 'norm':
        await runSQLFile(DATABASE_URL, 'sql/ddl_norm.sql');
        break;
      case 'dml':
        await runSQLFile(DATABASE_URL, 'sql/dml.sql');
        break;
      case 'all':
        await runSQLFile(DATABASE_URL, 'sql/ddl_init.sql');
        await runSQLFile(DATABASE_URL, 'sql/ddl_norm.sql');
        console.log('Now run: npm run etl');
        console.log('Then run: npm run db:dml');
        break;
      default:
        console.log('Usage: tsx src/db-init.ts [init|norm|dml|all]');
        process.exit(1);
    }
  } catch (error) {
    console.error('Database operation failed:', error);
    process.exit(1);
  }
}

main();