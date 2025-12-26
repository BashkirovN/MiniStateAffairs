import { MonitoringRepository } from "../db/monitoringRepository";
import { State, VideoSource } from "../db/types";

// TODO: Move this to a shared library
export enum LogLevel {
  INFO = "INFO",
  WARN = "WARN",
  ERROR = "ERROR"
}

interface PostgresInterval {
  minutes?: number;
  seconds?: number;
}

export class JobReporter {
  private runId: string | null = null;
  private counts = { discovered: 0, processed: 0, failed: 0 };
  private repo = new MonitoringRepository();

  constructor(
    private state: State,
    private source: VideoSource
  ) {}

  /**
   * Initializes a new job run record and establishes the tracking context.
   * This must be called before any logging or metric increments to ensure
   * data is correctly linked to a unique execution UUID.
   * @param executorName The identifier of the environment or service running the task
   */
  async startRun(executorName: string): Promise<void> {
    this.runId = await this.repo.createJobRun(
      this.state,
      this.source,
      executorName
    );
    await this.log(
      LogLevel.INFO,
      `Job started for: ${this.state}-${this.source}`
    );
  }

  /**
   * Records a timestamped log message to both the local console and the database.
   * @param level The severity level using the LogLevel enum (INFO, WARN, ERROR)
   * @param message The descriptive text to be recorded
   */
  async log(level: LogLevel, message: string): Promise<void> {
    if (!this.runId) return;

    const timestamp = new Date().toLocaleTimeString();

    const icons = {
      [LogLevel.INFO]: "ℹ️",
      [LogLevel.WARN]: "⚠️",
      [LogLevel.ERROR]: "❌"
    };
    console.log(`[${timestamp}] ${icons[level]} ${message}`);

    await this.repo.insertJobLog(this.runId, level, message);
  }

  /**
   * Increments the tally of items discovered during the initial scraping phase.
   * Automatically synchronizes the new count to the database metrics.
   * @param count The number of new items found (defaults to 1)
   */
  async incrementDiscovered(count: number = 1): Promise<void> {
    this.counts.discovered += count;
    await this.syncJobMetrics();
  }

  /**
   * Tracks a successfully processed video and triggers a database sync.
   */
  async incrementProcessed(): Promise<void> {
    this.counts.processed++;
    await this.syncJobMetrics();
  }

  /**
   * Tracks an item that failed to process and triggers a database sync.
   */
  async incrementFailed(): Promise<void> {
    this.counts.failed++;
    await this.syncJobMetrics();
  }

  /**
   * Internal helper to persist current in-memory counters to the database.
   * Ensures that the monitoring dashboard stays updated even if a job is long-running.
   */
  private async syncJobMetrics(): Promise<void> {
    if (!this.runId) return;
    await this.repo.updateJobMetrics(this.runId, this.counts);
  }

  /**
   * Concludes the job run, calculates final status, and captures terminal errors.
   * Triggers the final report generation to the console upon completion.
   * @param error Optional Error object if the job terminated due to an exception
   */
  async finishRun(error?: Error): Promise<void> {
    if (!this.runId) return;

    let status = "completed";
    let errorSummary = null;

    if (error) {
      status = "failed";
      errorSummary = error.message;
      await this.log(LogLevel.ERROR, `Fatal crash: ${error.message}`);
    } else if (this.counts.failed > 0) {
      status = "completed_with_errors";
    }

    // Save final state
    await this.repo.finalizeJobRun(this.runId, {
      status,
      counts: this.counts,
      errorSummary
    });

    // Output final results to console
    await this.printFinalSummary();
  }

  /**
   * Fetches the finalized run data and outputs a formatted summary table.
   * Provides immediate feedback on job performance, duration, and error details.
   */
  private async printFinalSummary(): Promise<void> {
    if (!this.runId) return;

    const summary = await this.repo.getJobRunSummary(this.runId);

    if (!summary) {
      console.warn(`[Report] No summary found for run ID: ${this.runId}`);
      return;
    }

    console.log(`\n${"=".repeat(40)}`);
    console.log(`JOB FINISHED: ${this.state}-${this.source}`);
    console.log(`${"=".repeat(40)}`);

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
   * Converts a Postgres Interval object into a human-readable string.
   * Handles edge cases for short-running jobs that finish in seconds.
   * @param duration The interval object from the database query
   */
  private formatPostgresInterval(
    duration: PostgresInterval | string | null | undefined
  ): string {
    if (!duration) return "0s";

    if (typeof duration === "string") return duration;

    const mins = duration.minutes || 0;
    const secs = duration.seconds || 0;
    return mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
  }
}
