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
   * Transcribes a video from an accessible URL (e.g., S3 public or presigned URL),
   * stores the transcript, and updates video status.
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
