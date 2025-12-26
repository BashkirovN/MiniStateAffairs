export enum State {
  MI = "MI"
}

export enum VideoSource {
  HOUSE = "house",
  SENATE = "senate"
}

export enum VideoStatus {
  QUEUED = "queued",
  PENDING = "pending", // Initial state, URL discovered but not downloaded
  DOWNLOADING = "downloading", // Currently being downloaded/uploaded to S3
  DOWNLOADED = "downloaded", // Video file is safe in S3
  TRANSCRIBING = "transcribing", // Transcription in progress
  COMPLETED = "completed", // Video uploaded AND transcribed
  FAILED = "failed", // Failed and scheduled for a retry
  PERMANENT_FAILURE = "permanent_failure" // Won't be picked up again
}

export interface VideoRow {
  id: string;
  state: string;
  source: VideoSource;
  external_id: string;
  slug: string;
  title: string;
  hearing_date: string;
  video_page_url: string; // The link to the website page
  original_video_url: string | null; // The direct direct link to the MP4/stream
  s3_key: string | null;
  status: VideoStatus;
  retry_count: number;
  last_error: string | null;
  created_at: string;
  updated_at: string;
}

export interface TranscriptRow {
  video_id: string;
  provider: string;
  language: string;
  text: string;
  raw_json: unknown | null;
  created_at: string;
}
