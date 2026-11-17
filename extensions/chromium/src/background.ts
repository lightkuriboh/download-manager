import { downloadUrl } from "./downloads.js";

const menu = "download-link";

chrome.runtime.onInstalled.addListener(() => {
  chrome.contextMenus.create({
    id: menu,
    title: "Download link",
    contexts: ["link"],
    targetUrlPatterns: ["http://*/*", "https://*/*"],
  }, () => {
    if (chrome.runtime.lastError) {
      console.warn("Could not register the download menu.");
    }
  });
});

chrome.contextMenus.onClicked.addListener(async (event) => {
  if (event.menuItemId !== menu || !event.linkUrl) return;
  try {
    await chrome.downloads.download({ url: downloadUrl(event.linkUrl), saveAs: true });
    await chrome.action.setBadgeText({ text: "" });
  } catch {
    await chrome.action.setBadgeText({ text: "!" });
  }
});
