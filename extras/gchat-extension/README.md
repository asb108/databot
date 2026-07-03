# Databot for Google Chat — Chrome Extension

A lightweight Chrome Extension that bridges Google Chat to your local [Databot](https://github.com/your-repo/databot) instance. No GCP project, no third-party services required.

## How It Works

1. You type `@databot <question>` in any Google Chat space.
2. The extension detects the trigger keyword in the DOM.
3. It sends the message to your local Databot gateway (`localhost:18790`).
4. Databot processes the request using its configured skills and tools.
5. The response is posted back into Google Chat via an Incoming Webhook.

## Installation

### Step 1: Load the Extension in Chrome
1. Open Chrome and go to `chrome://extensions/`.
2. Enable **Developer Mode** (top-right toggle).
3. Click **Load unpacked** and select this folder (`extras/gchat-extension/`).

### Step 2: Configure the Extension
1. Click the Databot robot icon in your Chrome toolbar.
2. Paste your GChat **Incoming Webhook URL** (e.g., `databot-dev`) into the settings field.
3. Click **Save Settings**. The status indicator will turn green when Databot is running.

### Step 3: Start Databot
```bash
# Terminal 1 — start the gateway
databot gateway --port 18790

# Terminal 2 — expose it publicly (needed for the Incoming Webhook to post back to GChat)
# (Only required if Databot is running locally)
ngrok http 18790
```

### Step 4: Chat!
Go to your Google Chat space and type:
```
@databot what is the status of ticket TS-96750?
```
Databot will reply in the same space via the webhook.

## Trigger Keyword

By default, the extension listens for messages containing `@databot` (case-insensitive).

## Files

| File | Purpose |
|---|---|
| `manifest.json` | Chrome Extension metadata and permissions |
| `content.js` | Watches the GChat DOM for `@databot` mentions |
| `popup.html` | Settings UI (webhook URL) |
| `popup.js` | Settings load/save logic and status check |
| `icon.png` | Extension icon |

## Permissions

| Permission | Reason |
|---|---|
| `https://chat.google.com/*` | Read new messages from the GChat page DOM |
| `http://localhost:18790/*` | Call local Databot gateway API |
| `storage` | Save webhook URL and settings locally |

All traffic stays on your machine. No credentials or message content is ever sent to an external server.
