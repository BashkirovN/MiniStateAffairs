import {
  TranscriptionResult,
  transcribeFromUrl
} from "../clients/deepgramClient";
import { TranscriptRepository } from "../db/transcriptRepository";
import { VideoRepository } from "../db/videoRepository";
import { VideoStatus } from "../db/types";

export enum TransProvider {
  DEEPGRAM = "deepgram"
}

export class TranscriptionService {
  constructor(
    private readonly transcriptRepo = new TranscriptRepository(),
    private readonly videoRepo = new VideoRepository()
  ) {}

  /**
   * Orchestrates the transcription process for a specific video using a remote URL.
   * * This method interfaces with the transcription provider, persists the resulting
   * text and raw metadata to the database, and transitions the video status
   * from 'transcribing' to either 'completed' or 'failed'.
   * * @param videoId The UUID of the video to be transcribed
   * @param url A publicly accessible or presigned S3 URL of the source media file
   * @returns A promise resolving to the structured transcription result, including text and provider metadata
   * @throws TranscriptionError if the provider fails to process the media
   */
  async transcribeVideoFromUrl(
    videoId: string,
    url: string
  ): Promise<TranscriptionResult> {
    try {
      await this.videoRepo.updateStatus(videoId, VideoStatus.TRANSCRIBING);

      const result = await transcribeFromUrl(url);

      await this.transcriptRepo.createTranscript({
        videoId,
        provider: TransProvider.DEEPGRAM,
        language: result.language,
        text: result.text,
        rawJson: result.raw
      });

      await this.videoRepo.updateStatus(videoId, VideoStatus.COMPLETED, {
        lastError: null
      });

      return result;
    } catch (err: any) {
      await this.videoRepo.updateStatus(videoId, VideoStatus.FAILED, {
        lastError: String(err?.message ?? err),
        incrementRetry: true
      });
      throw err;
    }
  }
}
