import { MonitoringRepository } from "../db/monitoringRepository";

async function main() {
  // Get the number of days from command line arguments, default to 7
  const args = process.argv.slice(2);
  const days = args[0] ? parseInt(args[0], 10) : 7;

  if (isNaN(days)) {
    console.error("Error: Please provide a valid number of days.");
    process.exit(1);
  }

  const repo = new MonitoringRepository();

  try {
    await repo.printLastDaysJobSummary(days);
    process.exit(0);
  } catch (error) {
    console.error("Failed to generate report:", error);
    process.exit(1);
  }
}

main();
