/**
 * Generates a URL-friendly, unique slug for a video record.
 * Normalizes the title by removing accents and special characters, and appends
 * the state, source, and hearing date to ensure a collision-resistant identifier.
 * @param state The geographic state abbreviation (e.g., 'MI')
 * @param source The branch or committee source (e.g., 'house')
 * @param title The descriptive title of the hearing or video
 * @param date The hearing date used to timestamp the slug
 * @returns A kebab-case string formatted as "state-source-title-date"
 * * @example
 * generateSlug('MI', 'House', 'Agriculture & Forestry!', new Date('2025-12-23'))
 * // returns "mi-house-agriculture-forestry-2025-12-23"
 */
export function generateSlug(
  state: string,
  source: string,
  title: string,
  date: Date
): string {
  const dateStr = date.toISOString().split("T")[0]; // YYYY-MM-DD

  const cleanTitle = title
    .toLowerCase()
    .normalize("NFD") // Handle accents/special chars
    .replace(/[\u0300-\u036f]/g, "") // Remove accents
    .replace(/[^a-z0-9\s-]/g, "") // Remove special characters
    .trim()
    .replace(/\s+/g, "-") // Replace spaces with hyphens
    .replace(/-+/g, "-"); // Remove double hyphens

  return `${state.toLowerCase()}-${source.toLowerCase()}-${cleanTitle}-${dateStr}`;
}
