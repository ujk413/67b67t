export const DEFAULT_BASE_URL = "https://67chat.ru/api";

export function normalizeBaseUrl(input?: string) {
  return (input?.trim() || DEFAULT_BASE_URL).replace(/\/+$/, "");
}

export function wsUrlFromApi(baseUrl: string) {
  const url = new URL(baseUrl.endsWith("/api") ? baseUrl.slice(0, -4) : baseUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.pathname = "/api/ws".replace("/api/api/", "/api/");
  return url.toString();
}

export function mediaUrl(baseUrl: string, mediaId?: string | null) {
  return mediaId ? `${normalizeBaseUrl(baseUrl)}/media/${mediaId}` : null;
}

export function mediaThumbUrl(baseUrl: string, mediaId?: string | null) {
  return mediaId ? `${normalizeBaseUrl(baseUrl)}/media/${mediaId}/thumb` : null;
}
