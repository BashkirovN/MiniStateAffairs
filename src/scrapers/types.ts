import { State, VideoSource } from "../db/types";

export interface DiscoveredVideo {
  state: State;
  source: VideoSource;

  /** The raw ID/filename from the government server (e.g., 'HAGRI-111325.mp4') */
  externalId: string;

  /** The human-readable URL identifier (e.g., 'mi-house-agri-111325-2025-12-23') */
  slug: string;

  title: string;
  hearingDate: Date;

  /** The page where the video was found */
  videoPageUrl: string;

  /** The direct link to the .mp4 or stream file */
  originalVideoUrl: string;
}
