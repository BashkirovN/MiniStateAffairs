// src/db/client.ts
import { Pool, QueryResultRow } from "pg";
import { loadConfig } from "../config/env";

const config = loadConfig();

export const pool = new Pool({
  connectionString: config.databaseUrl,
  max: 10,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
  ssl: {
    rejectUnauthorized: false // ok for now;
  }
});

pool.on("error", (err) => {
  console.error("Unexpected error on idle Postgres client", err);
});

export async function query<T extends QueryResultRow = QueryResultRow>(
  text: string,
  params: unknown[] = []
): Promise<{ rows: T[] }> {
  return pool.query<T>(text, params);
}

export async function shutdownPool(): Promise<void> {
  await pool.end();
}
