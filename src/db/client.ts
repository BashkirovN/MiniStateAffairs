import { Pool, QueryResultRow } from "pg";
import { loadConfig } from "../config/env";

const config = loadConfig();

export const pool = new Pool({
  connectionString: config.databaseUrl,
  max: 10,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
  ssl: {
    rejectUnauthorized: false
  }
});

pool.on("error", (err) => {
  console.error("Unexpected error on idle Postgres client", err);
});

/**
 * Executes a database query using a pooled client.
 * Leverages generic type parameters to ensure the returned rows match the expected
 * data structure, providing end-to-end type safety for SQL results.
 * @param text - The SQL query string with positional placeholders (e.g., $1, $2)
 * @param params - An array of values to bind to the query placeholders
 * @returns A promise resolving to an object containing the array of resulting rows
 */
export async function query<T extends QueryResultRow = QueryResultRow>(
  text: string,
  params: unknown[] = []
): Promise<{ rows: T[] }> {
  return pool.query<T>(text, params);
}

/**
 * Gracefully shuts down the PostgreSQL connection pool.
 * Drains all active connections and prevents new ones from being established.
 * Should be called during application termination to prevent memory leaks or hung processes.
 * @returns A promise that resolves once all pool connections have been closed
 */
export async function shutdownPool(): Promise<void> {
  await pool.end();
}
