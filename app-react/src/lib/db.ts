import { Pool } from 'pg';
import * as dotenv from 'dotenv';
import path from 'path';

// Load .env from the root of the project (dados-eleicoes-basicos)
dotenv.config({ path: path.resolve(process.cwd(), '../.env') });

const host = process.env.PGSQL_VECTOR_HOST;
const port = process.env.PGSQL_VECTOR_PORT;
const database = process.env.PGSQL_VECTOR_DATABASE;
const user = process.env.PGSQL_VECTOR_USERNAME;
const password = process.env.PGSQL_VECTOR_PASSWORD;

if (!host || !port || !database || !user) {
  throw new Error('Database connection variables (PGSQL_VECTOR_*) are missing. Check the .env file in the root directory.');
}

const pool = new Pool({
  host,
  port: parseInt(port, 10),
  database,
  user,
  password,
});

export const query = (text: string, params?: any[]) => {
  return pool.query(text, params);
};
