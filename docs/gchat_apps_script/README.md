# Connect databot to Google Chat WITHOUT Org Cloud Console Access

This is a workaround for orgs where you don't have GCP Cloud Console
permission. It uses **Google Apps Script** as a lightweight proxy Chat App
backed by a **free personal GCP project**.

```
User in GChat ──► Apps Script Chat App ──► databot /webhooks/gchat ──► LLM ──► response in chat
```

## Prerequisites

- A **personal Google account** (gmail) — for the free GCP project
- Your **org Google Workspace account** — to use the Chat App
- databot running and **publicly accessible** (ngrok for dev, k8s ingress for prod)

---

## Step-by-step Setup

### 1. Expose databot to the internet

**For development** (local machine):
```bash
# Install ngrok: https://ngrok.com
ngrok http 18790
# Note the https URL, e.g. https://abc123.ngrok-free.app
```

**For production** (k8s):
Your existing k8s service + ingress should work. Just ensure the
`/webhooks/gchat` endpoint is reachable.

### 2. Create a free GCP project (personal account)

1. Sign into [console.cloud.google.com](https://console.cloud.google.com)
   with your **personal Gmail** (not org account)
2. Create a new project (e.g., "databot-chat-app")
3. Note the **project number** (not project ID) from the dashboard

### 3. Enable Google Chat API

1. In your personal GCP project, go to **APIs & Services > Library**
2. Search for **Google Chat API** and enable it
3. Go to **Google Chat API > Configuration** tab and fill in:
   - **App name**: `databot`
   - **Avatar URL**: *(optional)*
   - **Description**: `AI data platform assistant`
   - **Functionality**: check both:
     - ✅ Receive 1:1 messages
     - ✅ Join spaces and group conversations
   - **Connection settings**: select **Apps Script project**
     *(you'll paste the deployment ID after step 5)*
   - **Visibility**: select **Specific people and groups**
     → add your org email addresses (e.g., `you@ninjavan.co`)
4. Save

### 4. Create the Apps Script

1. Go to [script.google.com](https://script.google.com) (sign in with your **org account**)
2. Create a new project, name it "databot"
3. Replace the contents of `Code.gs` with the file from this folder
4. In the editor, click the gear icon (⚙️ Project Settings):
   - Check **Show "appsscript.json" manifest file in editor**
   - Under **Google Cloud Platform (GCP) Project**, click **Change project**
   - Enter the **project number** from step 2
5. Go back to the Editor, open `appsscript.json`, replace with the file from this folder
6. **Update `DATABOT_URL`** in `Code.gs` to your ngrok/k8s URL

### 5. Deploy the Apps Script

1. Click **Deploy > New deployment**
2. Click the gear icon next to "Select type" → choose **Add-on**
3. Click **Deploy**
4. Copy the **Deployment ID**
5. Go back to GCP Console → Chat API → Configuration
6. Paste the **Deployment ID** in the connection settings field
7. Save

### 6. Add the bot in Google Chat

1. Open Google Chat (your org account)
2. Click the **+** next to "Spaces" or start a new DM
3. Search for "databot" — your Apps Script Chat App should appear
4. Send a message!

---

## How it works

1. User sends a message in Google Chat
2. GChat routes it to the Apps Script `onMessage()` function
3. Apps Script forwards the full event payload to your databot
   `POST /webhooks/gchat` endpoint via `UrlFetchApp.fetch()`
4. databot processes it synchronously through the agent loop (28s timeout)
5. The LLM response is returned to Apps Script
6. Apps Script returns it to Google Chat as the reply

## Enable/disable at runtime

Use the databot Settings UI (`http://localhost:18790/ui` → Settings → Channels)
to toggle Google Chat on/off. When disabled, the webhook returns HTTP 403 and
the Apps Script shows a friendly "channel not enabled" message.

## Troubleshooting

| Issue | Fix |
|-------|-----|
| "Couldn't reach databot" | Check ngrok is running and URL is correct in Code.gs |
| "Channel not enabled" | Toggle GChat on in databot Settings UI |
| Bot doesn't appear in Chat | Check Visibility in Chat API config — add your org email |
| Timeout errors | Complex queries may exceed 30s; try simpler questions |
| "Apps Script not authorized" | Re-deploy and re-authorize when prompted |

## Notes

- The Chat App runs under your **personal** GCP project's quota (free tier:
  ~14,400 API calls/day — more than enough)
- Apps Script execution timeout is 6 minutes (consumer) / 30 minutes
  (Workspace), but databot's handler times out at 28s to stay within GChat's
  30s response window
- No org admin approval needed if you use **Specific people** visibility
  (each user just needs to find and add the bot themselves)
- This approach works with both DMs and Spaces
