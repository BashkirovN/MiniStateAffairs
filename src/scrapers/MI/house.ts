import * as cheerio from "cheerio";
import { fetchWithRetry } from "../../utils/http";
import { DiscoveredVideo } from "../types";
import { State, VideoSource } from "../../db/types";
import { generateSlug } from "../utils";

const HOUSE_ARCHIVE_URL = "https://house.mi.gov/VideoArchive";

function parseHouseDate(dateText: string): Date | null {
  const match = dateText.match(
    /^(?:.*?\b)?(January|February|March|April|May|June|July|August|September|October|November|December)[a-z]*\s+(\d{1,2}),\s+(\d{4})/i
  );
  if (!match) return null;

  const [, monthName, day, year] = match;
  const month = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December"
  ].indexOf(monthName);
  return new Date(parseInt(year), month, parseInt(day));
}

export async function fetchHouseRecent(
  options: { daysBack?: number } = {}
): Promise<DiscoveredVideo[]> {
  const { daysBack = 30 } = options;
  const cutoff = new Date(Date.now() - daysBack * 24 * 60 * 60 * 1000);

  const res = await fetchWithRetry(HOUSE_ARCHIVE_URL, undefined, {
    maxAttempts: 5,
    baseDelayMs: 500,
    maxDelayMs: 8000
  });

  if (!res.ok) {
    throw new Error(`Failed to fetch House archive: ${res.status}`);
  }

  const html = await res.text();
  if (!html || html.length === 0) {
    throw new Error("Empty HTML response from House archive");
  }

  const $ = cheerio.load(html);

  const results: DiscoveredVideo[] = [];

  // Find ALL video links (even in collapsed sections)
  $('a[href*="/VideoArchivePlayer?video="]').each((_, el) => {
    const $link = $(el);
    const href = $link.attr("href");
    const dateText = $link.text().trim();

    if (!href || !dateText) return;

    const date = parseHouseDate(dateText);
    if (!date || date < cutoff || isNaN(date.getTime())) return;

    const videoMatch = href.match(/video=([^&\s]+)/);
    let rawVideoId = videoMatch ? videoMatch[1] : null;
    if (!rawVideoId) return;

    const cleanVideoId = rawVideoId.replace(".mp4", "");

    results.push({
      state: State.MI,
      source: VideoSource.HOUSE,
      externalId: rawVideoId, // e.g. "HAGRI-111325.mp4"
      slug: generateSlug(State.MI, VideoSource.HOUSE, cleanVideoId, date),
      title: cleanVideoId,
      hearingDate: date,
      videoPageUrl: `https://house.mi.gov${href}`,
      originalVideoUrl: `https://www.house.mi.gov/ArchiveVideoFiles/${rawVideoId}` // The actual MP4 link
    });
  });

  console.log(`Found ${results.length} recent House videos`);
  return results;
}
