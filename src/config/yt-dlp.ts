import { VideoSource } from "../db/types";

export const COMMON_CONFIG = [
  // Ensures yt-dlp only downloads the specific video URL provided,
  // even if that video is part of a list or channel.
  "--no-playlist",

  // Directs the video data to 'stdout' (standard output) instead of a file.
  // This allows your Node.js process to stream the data directly.
  "--output",
  "-",

  // Tells yt-dlp NOT to use .part files (writes directly)
  "--no-part",

  // Prevents yt-dlp from attempting to fix up broken streams
  "--fixup",
  "never",

  // Prevents yt-dlp from downloading thumbnails
  "--no-cache-dir",

  // Identifies the request as coming from a standard web browser.
  // Many gov servers block the default "yt-dlp" user-agent to prevent scraping.
  "--user-agent",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",

  // "Do Not Track" header. Some privacy-conscious servers expect this
  // or use it as part of a legitimate browser's header fingerprint.
  "--add-header",
  "DNT:1",

  // Metadata headers that tell the server this is a modern cross-origin fetch request.
  // Helps mimic a real Chrome/Safari browser interaction.
  "--add-header",
  "Sec-Fetch-Mode:cors",
  "--add-header",
  "Sec-Fetch-Site:cross-site",

  // If the server doesn't respond within 30 seconds, the connection is dropped.
  // Prevents a Node process from hanging indefinitely on a dead link.
  "--socket-timeout",
  "30",

  // If a video "fragment" (common in HLS/m3u8 streams) fails to download,
  // yt-dlp will try X more times before giving up.
  "--fragment-retries",
  "10",

  // If a request is rate-limited or fails, this forces the script to
  // wait X seconds before trying again to avoid being banned.
  "--retry-sleep",
  "5",

  // Uses yt-dlp's internal HLS downloader rather than calling an external
  // tool like ffmpeg for the download phase. Often more stable for streams.
  "--hls-prefer-native",
  //"--hls-use-mpegts", // Can be useful for some live streams (replaces "--hls-prefer-native")

  // Allows connection to older government servers that use outdated
  // SSL/TLS versions or configurations.
  "--legacy-server-connect",

  // Ignores SSL certificate validation. Many gov sites have expired
  // or self-signed certificates that would otherwise crash the download.
  "--no-check-certificate"
];

const PLATFORM_CONFIGS = {
  CASTUS: {
    referer: "https://cloud.castus.tv/",
    origin: "https://cloud.castus.tv",
    extraArgs: []
  },
  MICHIGAN_HOUSE: {
    referer: "https://www.house.mi.gov/",
    origin: "https://www.house.mi.gov",
    extraArgs: []
  }
};

const SOURCE_MAP: Record<VideoSource, keyof typeof PLATFORM_CONFIGS> = {
  [VideoSource.SENATE]: "CASTUS",
  [VideoSource.HOUSE]: "MICHIGAN_HOUSE"
};

/**
 * Constructs a specialized array of arguments for the yt-dlp downloader.
 * Combines global defaults with platform-specific headers (Referer, Origin) to bypass
 * common anti-bot and CORS restrictions on government media servers.
 * @param source - The branch of government providing the video source
 * @param url - The direct or page URL of the video to be downloaded
 * @returns A flat array of strings suitable for spawning a child process
 */
export function getYtDlpArgs(source: VideoSource, url: string): string[] {
  const platformKey = SOURCE_MAP[source];
  const config = PLATFORM_CONFIGS[platformKey];

  const platformArgs = [
    // Tells the server which page you "came from."
    // Gov platforms like Castus often check this to ensure the video is being played on their authorized site.
    "--add-header",
    `Referer:${config.referer}`,

    // Used for CORS (Cross-Origin Resource Sharing) requests.
    // It tells the server the domain of the script making the request.
    "--add-header",
    `Origin:${config.origin}`,

    ...(config.extraArgs || [])
  ];

  return [...COMMON_CONFIG, ...platformArgs, url];
}
