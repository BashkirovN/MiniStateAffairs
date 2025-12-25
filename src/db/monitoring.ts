import { query } from "./client";

export class MonitoringRepository {
  async getSummary(days: number): Promise<void> {
    const result = await query(
      `
      SELECT 
        state,
        DATE(start_time) as run_date,
        COUNT(id) as runs_count,
        SUM(items_discovered) as total_found,
        SUM(items_processed) as total_success,
        SUM(items_failed) as total_failures,
        ROUND(SUM(items_processed)::numeric / NULLIF(SUM(items_processed + items_failed), 0) * 100, 1) as success_rate_pct
      FROM job_runs
      WHERE start_time > NOW() - ($1 * INTERVAL '1 day')
      GROUP BY state, run_date
      ORDER BY run_date DESC, state;
      `,
      [days]
    );

    if (result && result.rows && result.rows.length > 0) {
      console.log(`\n--- Job Summary (Last ${days} Days) ---`);
      console.table(result.rows);
    } else {
      console.log("No job runs found for the specified period.");
    }
  }
}
