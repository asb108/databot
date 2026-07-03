/**
 * Google Apps Script — databot Chat App proxy.
 *
 * This script acts as a Google Chat App and forwards every message
 * to your databot gateway's /webhooks/gchat endpoint, returning the
 * LLM response synchronously back to the user.
 *
 * SETUP:
 * 1. Create a new Apps Script project at script.google.com
 * 2. Paste this code into Code.gs
 * 3. Update appsscript.json with the manifest (see appsscript.json)
 * 4. Create a FREE GCP project at console.cloud.google.com
 *    with your PERSONAL Google account (not the org account)
 * 5. In that GCP project: enable "Google Chat API"
 * 6. Configure Chat API (Configuration tab):
 *    - App name: databot
 *    - Avatar URL: (optional)
 *    - Description: AI data platform assistant
 *    - Functionality: check "Receive 1:1 messages" and "Join spaces"
 *    - Connection settings: choose "Apps Script project"
 *    - Deployment ID: paste from Apps Script (Deploy > New deployment)
 *    - Visibility: "Specific people" → add your org email(s)
 * 7. Link the Apps Script to the GCP project:
 *    Apps Script Editor > Project Settings > GCP Project > Change project
 *    Enter the GCP project number
 * 8. Deploy > New Deployment > select "Add-on" type > Deploy
 */

// ── Configuration ──────────────────────────────────────────────────
// Replace with your databot gateway URL (ngrok for dev, k8s ingress for prod)
var DATABOT_URL = "https://YOUR-NGROK-OR-DOMAIN.ngrok-free.app";

// Optional: API key if you have gateway auth enabled
var API_KEY = "";

// Timeout for the HTTP call in seconds (Apps Script max is 30s)
var TIMEOUT_SECONDS = 28;


// ── Chat App event handlers ────────────────────────────────────────

/**
 * Responds to a MESSAGE event in Google Chat.
 * @param {Object} event the event object from Google Chat
 * @return {Object} the response Card or text
 */
function onMessage(event) {
  var userMessage = event.message.argumentText || event.message.text || "";
  userMessage = userMessage.trim();

  if (!userMessage) {
    return { text: "I didn't receive any message. Try asking me something!" };
  }

  // Forward to databot
  var response = callDatabot(event);

  if (response) {
    return { text: response };
  }

  return { text: "Sorry, I couldn't reach the databot server. Is it running?" };
}

/**
 * Responds to an ADDED_TO_SPACE event.
 */
function onAddToSpace(event) {
  var msg = "👋 Hi! I'm **databot**, your AI data platform assistant.\n\n"
          + "I can help with:\n"
          + "• Pipeline monitoring & debugging\n"
          + "• SQL queries across your databases\n"
          + "• Data quality checks\n"
          + "• Kafka, Airflow, Spark operations\n\n"
          + "Just ask me anything!";
  return { text: msg };
}

/**
 * Responds to a REMOVED_FROM_SPACE event.
 */
function onRemoveFromSpace(event) {
  // No response needed
}


// ── Helper functions ───────────────────────────────────────────────

/**
 * Forwards the Google Chat event to the databot /webhooks/gchat endpoint.
 * The endpoint processes the message synchronously and returns the response.
 */
function callDatabot(event) {
  var url = DATABOT_URL + "/webhooks/gchat";

  // Build the payload exactly as Google Chat would send it
  var payload = {
    type: event.type || "MESSAGE",
    eventTime: event.eventTime,
    message: event.message,
    user: event.user,
    space: event.space,
    configCompleteRedirectUrl: event.configCompleteRedirectUrl || ""
  };

  var options = {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
    // Apps Script timeout (max 30s, we use 28 to be safe)
    // Note: UrlFetchApp doesn't have a direct timeout param; it uses
    // the script execution time limit (6 min for consumer, 30 min for Workspace)
  };

  // Add API key header if configured
  if (API_KEY) {
    options.headers = { "X-API-Key": API_KEY };
  }

  try {
    var httpResponse = UrlFetchApp.fetch(url, options);
    var status = httpResponse.getResponseCode();

    if (status === 200) {
      var body = JSON.parse(httpResponse.getContentText());
      return body.text || "Received a response but no text content.";
    } else if (status === 403) {
      return "⚠️ The Google Chat channel is not enabled on databot. "
           + "Enable it in Settings → Channels → Google Chat.";
    } else {
      Logger.log("databot returned HTTP " + status + ": " + httpResponse.getContentText());
      return "⚠️ databot returned an error (HTTP " + status + "). Check the server logs.";
    }
  } catch (e) {
    Logger.log("Error calling databot: " + e.toString());
    return null;  // triggers the "couldn't reach" fallback
  }
}
