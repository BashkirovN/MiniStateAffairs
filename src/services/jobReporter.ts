// JobReporter.ts
import { MonitoringRepository } from "../db/monitoringRepository";

export class JobReporter {
  private runId: string | null = null;
  private counts = { discovered: 0, processed: 0, failed: 0 };
  private repo = new MonitoringRepository();

  constructor(private state: string) {}

  async startRun(executorName: string) {
    this.runId = await this.repo.createJobRun(this.state, executorName);
    await this.log("INFO", `Job started for state: ${this.state}`);
  }

  async log(level: "INFO" | "WARN" | "ERROR", message: string) {
    if (!this.runId) return;

    const timestamp = new Date().toLocaleTimeString();
    const icon = level === "ERROR" ? "❌" : level === "WARN" ? "⚠️" : "ℹ️";
    console.log(`[${timestamp}] ${icon} ${message}`);

    await this.repo.insertLog(this.runId, level, message);
  }

  async incrementDiscovered(count: number = 1) {
    this.counts.discovered += count;
    await this.sync();
  }

  async incrementProcessed() {
    this.counts.processed++;
    await this.sync();
  }

  async incrementFailed() {
    this.counts.failed++;
    await this.sync();
  }

  private async sync() {
    if (!this.runId) return;
    await this.repo.updateMetrics(this.runId, this.counts);
  }

  async finishRun(error?: Error) {
    if (!this.runId) return;

    let status = "completed";
    let errorSummary = null;

    if (error) {
      status = "failed";
      errorSummary = error.message;
      await this.log("ERROR", `Fatal crash: ${error.message}`);
    } else if (this.counts.failed > 0) {
      status = "completed_with_errors";
    }

    // 1. Save final state
    await this.repo.finalizeJobRun(this.runId, {
      status,
      counts: this.counts,
      errorSummary
    });

    // 2. Output final results to console
    await this.printFinalSummary();
  }

  private async printFinalSummary() {
    if (!this.runId) return;

    const summary = await this.repo.getRunSummary(this.runId);

    console.log(`\n${"=".repeat(40)}`);
    console.log(`JOB FINISHED: ${this.state}`);
    console.log(`${"=".repeat(40)}`);

    // Using a small table for the key metrics
    console.table([
      {
        Status: summary.status,
        Found: summary.found,
        "Need Work": summary.ok + summary.fail,
        Success: summary.ok,
        Failed: summary.fail,
        Duration: this.formatPostgresInterval(summary.duration)
      }
    ]);

    if (summary.error_summary) {
      console.log(`\n❌ Error Detail: ${summary.error_summary}`);
    }
    console.log(`${"=".repeat(40)}\n`);
  }

  /**
   * Helper to make Postgres duration (e.g. {seconds: 12, milliseconds: 500}) readable
   */
  private formatPostgresInterval(duration: any): string {
    if (!duration) return "0s";
    const mins = duration.minutes || 0;
    const secs = duration.seconds || 0;
    return mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
  }
}
