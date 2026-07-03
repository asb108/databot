/**
 * Databot for Google Chat — Content Script v4
 *
 * All network calls are delegated to background.js to bypass Gmail's
 * strict Content Security Policy (which blocks direct localhost fetch).
 */

const TRIGGER = "@databot";
const POLL_MS = 2000;

let webhookUrl = "";
const sent = new Set();

// ── config ───────────────────────────────────────────────────────────────────

chrome.storage.sync.get(["webhookUrl"], (r) => {
    webhookUrl = r.webhookUrl || "";
    if (isRelevantFrame()) {
        console.log("[Databot] Content script ready. Trigger:", TRIGGER);
    }
});
chrome.storage.onChanged.addListener((c) => {
    if (c.webhookUrl) webhookUrl = c.webhookUrl.newValue;
});

// ── helpers ───────────────────────────────────────────────────────────────────

function isRelevantFrame() {
    const host = location.hostname;
    return host === "chat.google.com" || host === "mail.google.com";
}

function fingerprint(t) {
    return t.slice(0, 150).replace(/\s+/g, " ").trim().toLowerCase();
}

function spaceId() {
    // Works for both chat.google.com/room/... and mail.google.com/#chat/space/...
    return (
        location.pathname.match(/\/room\/([^/]+)/)?.[1] ||
        location.hash.match(/#chat\/space\/([^/]+)/)?.[1] ||
        "gchat-space"
    );
}

// ── core send (via background to bypass CSP) ─────────────────────────────────

function sendToDatabot(rawText) {
    const key = fingerprint(rawText);
    if (sent.has(key)) return;
    sent.add(key);
    setTimeout(() => sent.delete(key), 120_000);

    const question = rawText.replace(new RegExp(TRIGGER, "gi"), "").trim();
    if (!question) return;

    console.log("[Databot] ▶ Routing via background:", question.slice(0, 80));

    chrome.runtime.sendMessage(
        {
            type: "DATABOT_QUERY",
            message: question,
            sender: "gchat-user",
            chatId: spaceId(),
            webhookUrl,
        },
        (resp) => {
            if (chrome.runtime.lastError) {
                console.warn("[Databot] BG error:", chrome.runtime.lastError.message);
                return;
            }
            if (resp?.ok) {
                console.log("[Databot] ◀ Reply sent to GChat via webhook.");
            } else {
                console.error("[Databot] Query failed:", resp?.error);
            }
        }
    );
}

// ── scanner ───────────────────────────────────────────────────────────────────

const seenNodes = new WeakSet();

function scanElement(el) {
    if (!(el instanceof Element)) return;
    if (seenNodes.has(el)) return;
    const text = el.innerText || "";
    if (!text.toLowerCase().includes(TRIGGER)) return;
    seenNodes.add(el);
    sendToDatabot(text);
}

function scanAll() {
    // Broad selector — avoids relying on fragile GChat class names
    document
        .querySelectorAll("[data-message-id], [jsname], [role='listitem'], c-wiz, [jscontroller]")
        .forEach(scanElement);
}

// ── MutationObserver ──────────────────────────────────────────────────────────

const observer = new MutationObserver((muts) => {
    for (const mut of muts) {
        for (const node of mut.addedNodes) {
            if (node instanceof Element) {
                scanElement(node);
                node.querySelectorAll("[data-message-id],[jsname],[role='listitem']").forEach(scanElement);
            }
        }
    }
});

// ── bootstrap ─────────────────────────────────────────────────────────────────

function start() {
    if (!isRelevantFrame()) return;
    console.log("[Databot] Observer + poller active on", location.hostname);
    observer.observe(document.body, { childList: true, subtree: true });
    setInterval(scanAll, POLL_MS);
    scanAll(); // initial pass
}

if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
} else {
    start();
}
