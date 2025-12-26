import { DiscoveredVideo } from "../types";
import { State, VideoSource } from "../../db/types";
import { fetchWithRetry } from "../../utils/http";
import { generateSlug } from "../utils";

interface SenateApiResponse {
  record: number; // total records (2479)
  allFiles: Array<{
    _id: string; // "694572f06aeb4fb3964393c4"
    date: string; // "2025-12-19T15:44:48.377Z"
    original_date: string; // "2025-12-19T15:35:10.368Z"
    metadata: {
      filename: string; // "Senate Session 121825.mp4"
      description: string;
      "original filename": string; // duplicate of filename
      duration: string; // "10950.41"
    };
    user: string; // "61b3adc8124d7d000891ca5c"
  }>;
}

const MI_SENATE_ID = "61b3adc8124d7d000891ca5c";
const MI_SENATE_CDN_ID = "dlttx48mxf9m3";
const MI_SENATE_API_URL =
  "https://tf4pr3wftk.execute-api.us-west-2.amazonaws.com/default/api/all";

/**
 * Scrapes the Michigan Senate API to discover recent video records.
 * Iterates through paginated results from the Castus Cloud API, normalizing metadata
 * and constructing CDN URLs for HLS streams until the specified date cutoff is reached.
 * @param options - Configuration for the lookback window
 * @param options.daysBack - Number of days in the past to search for videos (defaults to 30)
 * @returns A promise resolving to an array of discovered video objects ready for ingestion
 */
export async function fetchRecent(
  options: { daysBack?: number } = {}
): Promise<DiscoveredVideo[]> {
  const { daysBack = 30 } = options;
  const cutoff = new Date(Date.now() - daysBack * 24 * 60 * 60 * 1000);

  const results: DiscoveredVideo[] = [];
  let page = 1;
  let consecutiveFailures = 0;
  const MAX_FAILURES = 2;
  const MAX_PAGES_TO_FETCH = 20;
  const RESULTS_PER_PAGE = 50;
  let shouldStop = false;

  while (
    page <= MAX_PAGES_TO_FETCH &&
    consecutiveFailures < MAX_FAILURES &&
    !shouldStop
  ) {
    const pageBody = {
      _id: MI_SENATE_ID,
      page,
      results: RESULTS_PER_PAGE
    };

    try {
      const res = await fetchWithRetry(MI_SENATE_API_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json;charset=UTF-8",
          Origin: "https://cloud.castus.tv",
          Referer: "https://cloud.castus.tv/",
          "User-Agent":
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        },
        body: JSON.stringify(pageBody)
      });

      if (!res.ok) throw new Error(`HTTP ${res.status}`);

      const data: SenateApiResponse = await res.json();
      const pageVideos = data.allFiles || [];

      for (const file of pageVideos) {
        const dateStr = file.date || file.original_date;
        const date = new Date(dateStr);

        if (!isNaN(date.getTime()) && date < cutoff) {
          shouldStop = true;
          break;
        }

        if (!isNaN(date.getTime()) && date > cutoff) {
          const rawTitle = (
            file.metadata?.filename || `Senate ${file._id.slice(-6)}`
          ).replace(".mp4", "");
          const videoPageUrl = `https://cloud.castus.tv/vod/misenate/video/${file._id}`;

          const originalVideoUrl = `https://${MI_SENATE_CDN_ID}.cloudfront.net/outputs/${file._id}/Default/HLS/out.m3u8`;

          results.push({
            state: State.MI,
            source: VideoSource.SENATE,
            externalId: file._id,
            slug: generateSlug(State.MI, VideoSource.SENATE, rawTitle, date),
            title: rawTitle,
            hearingDate: date,
            videoPageUrl,
            originalVideoUrl
          });
        }
      }
      consecutiveFailures = 0;
      if (shouldStop) break;
    } catch (error: any) {
      consecutiveFailures++;
      console.log(`❌ Page ${page} failed: ${error.message}`);
    }
    page++;
  }

  return results;
}
