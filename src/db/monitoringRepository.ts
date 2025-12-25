import { query } from "./client";

export class MonitoringRepository {
  /**
   * Creates the initial record for a job run
   */
  async createJobRun(
    state: string,
    source: string,
    executor: string
  ): Promise<string> {
    const res = await query<{ id: string }>(
      `INSERT INTO job_runs (state, source, executor, status) 
       VALUES ($1, $2, $3, 'running') RETURNING id`,
      [state, source, executor]
    );
    return res.rows[0].id;
  }

  /**
   * Adds a log entry linked to a specific run
   */
  async insertLog(
    runId: string,
    level: string,
    message: string
  ): Promise<void> {
    await query(
      `INSERT INTO job_logs (run_id, level, message) VALUES ($1, $2, $3)`,
      [runId, level, message]
    );
  }

  /**
   * Updates metrics in real-time
   */
  async updateMetrics(
    runId: string,
    counts: { discovered: number; processed: number; failed: number }
  ): Promise<void> {
    await query(
      `UPDATE job_runs 
       SET items_discovered = $1, 
           items_processed = $2, 
           items_failed = $3 
       WHERE id = $4`,
      [counts.discovered, counts.processed, counts.failed, runId]
    );
  }

  /**
   * Finalizes the job run with status and optional error summary
   */
  async finalizeJobRun(
    runId: string,
    data: {
      status: string;
      counts: { discovered: number; processed: number; failed: number };
      errorSummary: string | null;
    }
  ): Promise<void> {
    await query(
      `UPDATE job_runs 
       SET end_time = NOW(), 
           status = $1, 
           items_discovered = $2, 
           items_processed = $3, 
           items_failed = $4,
           error_summary = $5
       WHERE id = $6`,
      [
        data.status,
        data.counts.discovered,
        data.counts.processed,
        data.counts.failed,
        data.errorSummary,
        runId
      ]
    );
  }

  /**
   * Returns a summary of a specific job run
   */
  async getRunSummary(runId: string) {
    const res = await query(
      `SELECT 
        state, 
        source,
        status, 
        items_discovered as found, 
        items_processed as ok, 
        items_failed as fail,
        end_time - start_time as duration,
        error_summary
       FROM job_runs WHERE id = $1`,
      [runId]
    );
    return res.rows[0];
  }

  /**
   * Gets a summary of job runs for the last X days
   */
  async getLastDaysSummary(days: number) {
    const res = await query(
      `
      SELECT 
        state,
        source,
        DATE(start_time) as run_date,
        COUNT(id) as runs_count,
        SUM(items_discovered) as total_found,
        SUM(items_processed) as total_success,
        SUM(items_failed) as total_failures,
        ROUND(SUM(items_processed)::numeric / NULLIF(SUM(items_processed + items_failed), 0) * 100, 1) as success_rate_pct
      FROM job_runs
      WHERE start_time > NOW() - ($1 * INTERVAL '1 day')
      GROUP BY state, source, run_date
      ORDER BY run_date DESC, state, source;
      `,
      [days]
    );

    return res.rows;
  }

  /*
   * Prints a summary of job runs for the last X days
   */
  async printLastDaysSummary(days: number) {
    const rows = await this.getLastDaysSummary(days);

    if (!rows || rows.length === 0) {
      console.log(`\n--- No job data found for the last ${days} days ---`);
      return;
    }

    const reportData = rows.map((row: any) => {
      const rate = parseFloat(row.success_rate_pct || 0);
      let statusIcon = "🟢";
      if (rate < 95) statusIcon = "🟡";
      if (rate < 80) statusIcon = "🔴";

      return {
        Stat: statusIcon,
        State: row.state,
        Source: row.source || "N/A",
        Date: new Date(row.run_date).toISOString().split("T")[0],
        Runs: parseInt(row.runs_count),
        Found: parseInt(row.total_found),
        OK: parseInt(row.total_success),
        Fail: parseInt(row.total_failures),
        "Success %": `${rate}%`
      };
    });

    console.log(`\n=== SYSTEM MONITORING SUMMARY (Last ${days} Days) ===`);
    console.table(reportData);
    console.log(`Legend: 🟢 >95% | 🟡 >80% | 🔴 <80% \n`);
  }
}
