import { loadConfig } from "../config/env";
import { VideoRepository } from "../db/videoRepository";
import { shutdownPool } from "../db/client";
import { TranscriptionService } from "../services/transcriptionService";
import { TranscriptRepository } from "../db/transcriptRepository";
import { IngestService } from "../services/ingestService";
import { MonitoringRepository } from "../db/monitoring";

export async function runIngestJob(): Promise<void> {
  const config = loadConfig();
  console.log("Ingest starting. Bucket:", config.s3Bucket);

  const videoRepo = new VideoRepository();
  const transcriptionService = new TranscriptionService();
  const transcriptRepo = new TranscriptRepository();

  // Inject all dependencies
  const ingestService = new IngestService(
    videoRepo,
    transcriptRepo,
    transcriptionService
  );

  // DB smoke test
  // await videos.upsertDiscoveredVideo({
  //   id: "test:123",
  //   source: "house",
  //   title: "Test Hearing",
  //   hearingDate: new Date(),
  //   videoPageUrl: "https://example.com"
  // });

  // S3 smoke test: write a tiny text file
  // await uploadObject({
  //   key: "healthcheck/ingest-test.txt",
  //   body: "Hello S3 from ingest job",
  //   contentType: "text/plain"
  // });

  /*
  // Transcribe test
  const videoId = "deepgram-test:1";

  await videos.upsertDiscoveredVideo({
    id: videoId,
    source: "house",
    title: "Deepgram Test Hearing",
    hearingDate: new Date(),
    videoPageUrl: "https://example.com"
  });

  // Replace this with a real, publicly accessible audio/video file URL
  const testUrl = "https://dpgr.am/spacewalk.wav"; // Deepgram example file in some docs.[web:24][web:105]

  const result = await transcriptionService.transcribeVideoFromUrl(
    videoId,
    testUrl
  );

  console.log("Transcript language:", result.language);
  console.log("Transcript preview:", result.text.slice(0, 200));

  await shutdownPool();
  */

  // Discover videos
  //await ingestService.discoverNewVideos(5);

  // Upload video from url to S3

  // Reset counts
  // const videos = await videoRepo.getAllVideos(10);
  // for (const vid of videos) {
  //   await videoRepo.resetRetryCount(vid.id);
  // }

  // curl "https://house.mi.gov/VideoArchivePlayer?video=Session-122325.mp4" | grep -i 'src.*mp4\|video\|hls'
  // const processable = await videoRepo.findProcessable(2000);

  // console.log("Processable videos in DB:", processable.length);

  // if (processable.length > 0) {
  //   const problematicSenateVideoId = "e85450ef-605a-40d0-8a84-693f9c2653d6";
  //   const shortSenateVideoId = "8f35d2bd-b212-4a98-a470-ec8ea2899190";
  //   const shortHouseVideoId = "f425f89c-bc74-4f73-9a8a-c06cbc6df9f5";
  //   for (const video of processable) {
  //     console.log(video.source, video.id, video.external_id);

  //     if (video.id === shortHouseVideoId) {
  //       console.log("Video: ", video);

  //       const validSenateUrl =
  //         "https://cloud.castus.tv/vod/misenate/video/694572f06aeb4fb3964393c4";

  //       const validDownloadUrl =
  //         "https://www.house.mi.gov/ArchiveVideoFiles/Session-122325.mp4";
  //       const videoUrl =
  //         "https://www.house.mi.gov/ArchiveVideoFiles/HAGRI-111325.mp4";

  //       const s3videoUrl = await ingestService.uploadStep(video.id);

  //       // Transcribe and save to DB
  //       if (s3videoUrl) {
  //         console.log(`Video is live at: ${s3videoUrl}`);
  //         await ingestService.transcribeStep(video.id);
  //       }
  //     }
  //   }
  // }

  const monitoringRepository = new MonitoringRepository();
  await monitoringRepository.getSummary(7);

  await shutdownPool();
}

/*
Working senate curl:

curl 'https://dlttx48mxf9m3.cloudfront.net/outputs/694560d93eb8668433824782/Default/HLS/out.m3u8' \
  -H 'sec-ch-ua-platform: "macOS"' \
  -H 'Referer: https://cloud.castus.tv/' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36' \
  -H 'sec-ch-ua: "Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"' \
  -H 'DNT: 1' \
  -H 'sec-ch-ua-mobile: ?0'


 yt-dlp --referer "https://misenate.pagedemo.co/" --add-header "Origin: https://misenate.pagedemo.co" -g "https://dlttx48mxf9m3.cloudfront.net/outputs/694560d93eb8668433824782/Default/HLS/out.m3u8"
ffmpeg -i "https://dlttx48mxf9m3.cloudfront.net/outputs/694560d93eb8668433824782/Default/HLS/out.m3u8" -c copy test.mp4
 */
