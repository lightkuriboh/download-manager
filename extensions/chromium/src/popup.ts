import { actionsFor, downloadUrl, type Action } from "./downloads.js";

const form = document.querySelector<HTMLFormElement>("#add")!;
const url = document.querySelector<HTMLInputElement>("#url")!;
const status = document.querySelector<HTMLParagraphElement>("#status")!;
const jobs = document.querySelector<HTMLUListElement>("#jobs")!;
const number = new Intl.NumberFormat();
const pending = new Set<number>();
let reading = false;
let adding = false;

function render(items: chrome.downloads.DownloadItem[]) {
  const selected = document.activeElement instanceof HTMLButtonElement
    ? document.activeElement.dataset.control
    : undefined;
  const scroll = jobs.scrollTop;
  const rows = items.map((item) => {
    const row = document.createElement("li");
    const filename = document.createElement("span");
    filename.className = "filename";
    filename.textContent = item.filename.split(/[\\/]/).pop() || "Download";
    const detail = document.createElement("span");
    detail.className = "detail";
    const state = item.paused ? "Paused" : {
      in_progress: "Downloading",
      interrupted: "Interrupted",
      complete: "Complete",
    }[item.state];
    detail.textContent = `${state} · ${number.format(item.bytesReceived)} bytes`;
    row.append(filename, detail);
    if (item.state === "in_progress" && item.totalBytes > 0) {
      const progress = document.createElement("progress");
      progress.setAttribute("aria-label", `Progress for ${filename.textContent}`);
      progress.max = item.totalBytes;
      progress.value = item.bytesReceived;
      row.append(progress);
    }
    const controls = document.createElement("div");
    controls.className = "controls";
    for (const action of actionsFor(item)) {
      const button = document.createElement("button");
      button.dataset.control = `${item.id}:${action}`;
      button.textContent = action;
      button.disabled = pending.has(item.id);
      button.addEventListener("click", () => void change(action, item.id));
      controls.append(button);
    }
    row.append(controls);
    return row;
  });
  if (!rows.length) {
    const empty = document.createElement("li");
    empty.textContent = "No recent downloads";
    rows.push(empty);
  }
  jobs.replaceChildren(...rows);
  jobs.scrollTop = scroll;
  if (selected) {
    [...jobs.querySelectorAll("button")]
      .find((button) => button.dataset.control === selected)?.focus({ preventScroll: true });
  }
}

async function refresh() {
  if (reading) return;
  reading = true;
  try {
    render(await chrome.downloads.search({ orderBy: ["-startTime"], limit: 50 }));
  } catch {
    status.textContent = "Could not read the browser download list.";
  } finally {
    reading = false;
  }
}

async function change(action: Action, id: number) {
  if (pending.has(id)) return;
  pending.add(id);
  status.textContent = "";
  try {
    await chrome.downloads[action](id);
  } catch {
    status.textContent = `The browser could not ${action} this download.`;
  } finally {
    pending.delete(id);
    void refresh();
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (adding) return;
  status.textContent = "";
  adding = true;
  try {
    const requested = url.value;
    await chrome.downloads.download({ url: downloadUrl(requested), saveAs: true });
    if (url.value === requested) url.value = "";
    void refresh();
  } catch {
    status.textContent = "Could not start the download. Check the HTTP(S) URL and browser settings.";
  } finally {
    adding = false;
  }
});

void chrome.action.getBadgeText({}).then(async (badge) => {
  if (badge !== "!") return;
  status.textContent = "Could not start the link download from the context menu.";
  await chrome.action.setBadgeText({ text: "" });
}).catch(() => {});
void refresh();
setInterval(() => void refresh(), 1500);
