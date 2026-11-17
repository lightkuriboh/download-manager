export function downloadUrl(input: string): string {
  const url = new URL(input.trim());
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password) {
    throw new Error("Enter an HTTP or HTTPS URL without embedded credentials.");
  }
  return url.href;
}

export type Action = "pause" | "resume" | "cancel";

export function actionsFor(item: Pick<chrome.downloads.DownloadItem, "state" | "paused" | "canResume">): Action[] {
  if (item.state === "in_progress") {
    if (!item.paused) return ["pause", "cancel"];
    return item.canResume ? ["resume", "cancel"] : ["cancel"];
  }
  return item.state === "interrupted" && item.canResume ? ["resume"] : [];
}
