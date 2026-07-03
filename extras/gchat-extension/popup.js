// Route health check through background service worker to bypass CSP
function checkDatabotStatus() {
    const dot = document.getElementById("statusDot");
    const text = document.getElementById("statusText");
    text.textContent = "Checking Databot…";
    chrome.runtime.sendMessage({ type: "HEALTH_CHECK" }, (resp) => {
        if (chrome.runtime.lastError) {
            dot.classList.remove("online");
            text.textContent = "Extension error — reload the page";
            return;
        }
        if (resp?.ok) {
            dot.classList.add("online");
            text.textContent = "Databot is running ✓";
        } else {
            dot.classList.remove("online");
            text.textContent = "Databot offline — start gateway first";
        }
    });
}

// Load saved settings into the form
chrome.storage.sync.get(["webhookUrl"], (result) => {
    document.getElementById("webhookUrl").value = result.webhookUrl || "";
});

// Save settings
document.getElementById("saveBtn").addEventListener("click", () => {
    const webhookUrl = document.getElementById("webhookUrl").value.trim();
    chrome.storage.sync.set({ webhookUrl }, () => {
        const msg = document.getElementById("savedMsg");
        msg.textContent = "✓ Settings saved!";
        setTimeout(() => (msg.textContent = ""), 2000);
    });
});

// Check status on popup open
checkDatabotStatus();
