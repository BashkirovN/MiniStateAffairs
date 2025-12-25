/**
 * Generates a URL-friendly, unique slug for a video.
 * Example: "mi-house-agriculture-committee-2025-12-23"
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
