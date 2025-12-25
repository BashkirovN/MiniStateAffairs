import { query } from "../db/client";
import { State } from "../db/types";

export class JobReporter {
  private runId: string | null = null;
  private counts = { discovered: 0, processed: 0, failed: 0 };

  constructor(private state: State) {}

  async startRun(executorName: string) {
    const res = await query<{ id: string }>(
      `INSERT INTO job_runs (state, executor) VALUES ($1, $2) RETURNING id`,
      [this.state, executorName]
    );
    this.runId = res.rows[0].id;
    await this.log("INFO", `Job started for state: ${this.state}`);
  }

  async log(level: "INFO" | "WARN" | "ERROR", message: string) {
    if (!this.runId) return;
    console.log(`[${level}] ${message}`);
    await query(
      `INSERT INTO job_logs (run_id, level, message) VALUES ($1, $2, $3)`,
      [this.runId, level, message]
    );
  }

  incrementDiscovered(count: number = 1) {
    this.counts.discovered += count;
  }
  incrementProcessed() {
    this.counts.processed++;
  }
  incrementFailed() {
    this.counts.failed++;
  }

  async finishRun(error?: Error) {
    if (!this.runId) return;

    let status = "completed";
    let errorSummary = null;

    if (error) {
      status = "failed";
      errorSummary = error.message;
    } else if (this.counts.failed > 0) {
      status = "completed_with_errors";
    }

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
        status,
        this.counts.discovered,
        this.counts.processed,
        this.counts.failed,
        errorSummary,
        this.runId
      ]
    );
  }
}
