# Design decisions and questions

## Decisions

- **AWS S3** and **RDS** + plain **SQL** were picked to get a refresher on the stack used by StateAffairs.
- **yt-dlp** was picked for its reliable streaming when dealing with brittle connections.
- Some government websites have issues with certificates. Having a necessary certificate in `certs` solved the problem for Michigan House.

## Questions

- If we only need transcripts, could we use Deepgram to transcribe videos by url instead of downloading them?

Example workflow (step‑by‑step)
Cron triggers node ingest.js every 10 minutes.

ingest.js:

Fetch latest House/Senate listings, parse entries and derive IDs for the last 60 days.

Upsert each into videos with status='pending' when new.

ingest.js selects up to N videos where status IN ('pending','failed') and retry_count < N_MAX.

For each selected video:

Mark status='downloading', attempt download.

On success, mark status='downloaded'; on failure, status='failed', increment retry_count.

For status='downloaded' videos:

Mark status='transcribing', call Whisper or a transcription API.

On success, store transcript and mark status='completed'.

On failure, mark status='failed', increment retry_count.

Subsequent cron runs continue from DB state, picking up any missed or failed videos automatically.

## Database Setup

1. Connect to RDS:
   `psql -h <host> -U <user> -d <database>`
2. Initialize schema:
   `\i db/schema.sql`
