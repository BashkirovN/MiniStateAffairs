#!/usr/bin/env npx tsx
import { MonitoringRepository } from "../db/monitoringRepository";
import { State, VideoSource } from "../db/types";
import { VideoRepository } from "../db/videoRepository";

async function main() {
  // Get the number of days from command line arguments, default to 7
  const args = process.argv.slice(2);
  const command = args[0]; // e.g., 'abandoned' or undefined
  //const days = args[0] ? parseInt(args[0], 10) : 7;

  const videoRepo = new VideoRepository();
  const monitoringRepo = new MonitoringRepository();

  try {
    if (command === "abandoned") {
      // Logic for Abandoned Report
      // Usage: npm run report abandoned MI senate
      const state = (args[1] as State) || State.MI;
      const source = (args[2] as VideoSource) || VideoSource.SENATE;

      await videoRepo.printAbandonedReport(state, source);
    } else {
      // Default: Logic for Summary Report
      // Usage: npm run report 30
      const days = args[0] ? parseInt(args[0], 10) : 7;
      if (isNaN(days)) {
        console.error("Error: Please provide a valid number of days.");
        process.exit(1);
      }
      await monitoringRepo.printLastDaysJobSummary(days);
    }
    process.exit(0);
  } catch (err: unknown) {
    console.error(
      "💥 Command failed:",
      err instanceof Error ? err.message : String(err)
    );
    process.exit(1);
  }
}

main();
