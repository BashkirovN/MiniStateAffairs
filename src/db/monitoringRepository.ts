import { LogLevel } from "../services/jobReporter";
import { query } from "./client";

export interface JobRunSummary {
  state: string;
  source: string;
  status: "pending" | "running" | "completed" | "failed";
  found: number;
  ok: number;
  fail: number;
  duration: string | { hours?: number; minutes?: number; seconds?: number };
  error_summary: string | null;
}

export interface DailyStats {
  state: string;
  source: string;
  run_date: Date;
  runs_count: number;
  total_found: number;
  total_success: number;
  total_failures: number;
  success_rate_pct: number | null;
}

export class MonitoringRepository {
  /**
   * Initializes a new job execution record in the tracking table.
   * Sets the initial status to 'running' and returns the generated UUID for subsequent logging.
   * @param state The geographic state being processed (e.g., 'MI')
   * @param source The specific branch or source (e.g., 'senate')
   * @param executor The name or identifier of the service/worker running the job
   * @returns The unique UUID of the newly created job run
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
   * Appends a granular log message to a specific job run.
   * Used for auditing the step-by-step progress of a scraper or processor.
   * @param runId The UUID of the associated job run
   * @param level The severity level using the LogLevel enum (INFO, WARN, ERROR)
   * @param message The descriptive log content
   */
  async insertJobLog(
    runId: string,
    level: LogLevel,
    message: string
  ): Promise<void> {
    await query(
      `INSERT INTO job_logs (run_id, level, message) VALUES ($1, $2, $3)`,
      [runId, level, message]
    );
  }

  /**
   * Updates the progress counters for a job run in real-time.
   * This allows for monitoring long-running jobs before they are finalized.
   * @param runId The UUID of the job run to update
   * @param counts An object containing discovered, processed, and failed tallies
   */
  async updateJobMetrics(
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
   * Marks a job run as complete and records final metrics and timestamps.
   * Captures an optional error summary if the job terminated due to an exception.
   * @param runId The UUID of the job run to close
   * @param data The final status, counters, and any terminal error messages
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
   * Retrieves a high-level summary of a specific job run, including duration.
   * Useful for API responses or detailed single-job CLI inspections.
   * @param runId The UUID of the job run
   */
  async getJobRunSummary(runId: string): Promise<JobRunSummary | null> {
    const { rows } = await query<JobRunSummary>(
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
    return rows[0] || null;
  }

  /**
   * Aggregates job performance statistics over a rolling window of days.
   * Groups results by state and source to calculate success rates and throughput.
   * @param days The number of days to look back from the current time
   */
  async getLastDaysJobSummary(days: number): Promise<DailyStats[]> {
    // In PostgreSQL, COUNT and SUM returned as strings. Need to cast to int/float
    const { rows } = await query<DailyStats>(
      `
      SELECT 
        state,
        source,
        DATE(start_time) as run_date,
        COUNT(id)::int as runs_count,     
        SUM(items_discovered)::int as total_found,
        SUM(items_processed)::int as total_success,
        SUM(items_failed)::int as total_failures,
        ROUND(SUM(items_processed)::numeric / NULLIF(SUM(items_processed + items_failed), 0) * 100, 1)::float as success_rate_pct
      FROM job_runs
      WHERE start_time > NOW() - ($1 * INTERVAL '1 day')
      GROUP BY state, source, run_date
      ORDER BY run_date DESC, state, source;
      `,
      [days]
    );
    return rows;
  }

  /**
   * Generates and prints a visual health report of system jobs to the console.
   * Includes color-coded status indicators (🟢/🟡/🔴) based on the success percentage.
   * @param days The lookback period for the report in days
   */
  async printLastDaysJobSummary(days: number): Promise<void> {
    const rows = await this.getLastDaysJobSummary(days);

    if (!rows || rows.length === 0) {
      console.log(`\n--- No job data found for the last ${days} days ---`);
      return;
    }

    const reportData = rows.map((row: DailyStats) => {
      const rate = row.success_rate_pct || 0;
      let statusIcon = "🟢";
      if (rate < 95) statusIcon = "🟡";
      if (rate < 80) statusIcon = "🔴";

      return {
        Stat: statusIcon,
        State: row.state,
        Source: row.source || "N/A",
        Date: new Date(row.run_date).toISOString().split("T")[0],
        Runs: row.runs_count,
        Found: row.total_found,
        OK: row.total_success,
        Fail: row.total_failures,
        "Success %": `${rate}%`
      };
    });

    console.log(`\n=== SYSTEM MONITORING SUMMARY (Last ${days} Days) ===`);
    console.table(reportData);
    console.log(`Legend: 🟢 >95% | 🟡 >80% | 🔴 <80% \n`);
  }
}
