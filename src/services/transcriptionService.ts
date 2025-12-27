import {
  TranscriptionResult,
  transcribeFromUrl as DeepgramTranscribeFromUrl
} from "../clients/deepgramClient";

export enum TransProvider {
  DEEPGRAM = "deepgram"
}

export class TranscriptionService {
  /**
   * Orchestrates the transcription process for a specific video using a remote URL.
   * * This method interfaces with the transcription provider.
   * @param url A publicly accessible or presigned S3 URL of the source media file
   * @param provider The transcription provider to use
   * @returns A promise resolving to the structured transcription result, including text and provider metadata
   * @throws TranscriptionError if the provider fails to process the media
   */
  async transcribeVideoFromUrl(
    url: string,
    provider: TransProvider = TransProvider.DEEPGRAM
  ): Promise<TranscriptionResult> {
    switch (provider) {
      case TransProvider.DEEPGRAM:
        return await DeepgramTranscribeFromUrl(url);
      default:
        throw new Error(`Unsupported transcription provider: ${provider}`);
    }
  }
}
