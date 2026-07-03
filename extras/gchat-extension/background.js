/**
 * Background Service Worker for Databot Chrome Extension.
 *
 * All fetch calls to localhost go through here to bypass Gmail's
 * strict Content Security Policy, which blocks direct localhost
 * requests from content scripts injected into mail.google.com.
 */

const DATABOT_API = "http://localhost:18790/api/v1/message";
const HEALTH_URL = "http://localhost:18790/health";

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
    if (msg.type === "DATABOT_QUERY") {
        handleQuery(msg).then(sendResponse).catch((e) => {
            sendResponse({ ok: false, error: String(e) });
        });
        return true; // keep message channel open for async response
    }

    if (msg.type === "HEALTH_CHECK") {
        fetch(HEALTH_URL, { signal: AbortSignal.timeout(3000) })
            .then((r) => sendResponse({ ok: r.ok }))
            .catch(() => sendResponse({ ok: false }));
        return true;
    }
});

async function handleQuery({ message, sender, chatId, webhookUrl }) {
    const res = await fetch(DATABOT_API, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message, sender, chat_id: chatId }),
    });

    if (!res.ok) return { ok: false, error: `API ${res.status}` };

    const data = await res.json();
    const response = data.response || "";

    // Send reply via GChat Incoming Webhook (also from background — no CSP issues)
    if (webhookUrl && response) {
        try {
            await fetch(webhookUrl, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ text: `*Databot:* ${response}` }),
            });
        } catch (e) {
            console.warn("[Databot BG] Webhook error:", e);
        }
    }

    return { ok: true, response };
}
