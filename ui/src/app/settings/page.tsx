"use client";

import { useState } from "react";
import { Settings, Save, RotateCcw } from "lucide-react";

export default function SettingsPage() {
  const [apiUrl, setApiUrl] = useState(
    process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"
  );
  const [apiKey, setApiKey] = useState("");
  const [saved, setSaved] = useState(false);

  const handleSave = () => {
    // Settings are stored in-memory for now; in a real app, use localStorage or cookies
    if (typeof window !== "undefined") {
      localStorage.setItem("databot_api_url", apiUrl);
      localStorage.setItem("databot_api_key", apiKey);
    }
    setSaved(true);
    setTimeout(() => setSaved(false), 2000);
  };

  const handleReset = () => {
    setApiUrl("http://localhost:8000");
    setApiKey("");
    if (typeof window !== "undefined") {
      localStorage.removeItem("databot_api_url");
      localStorage.removeItem("databot_api_key");
    }
  };

  return (
    <div className="flex h-full flex-col">
      <header className="flex h-14 items-center border-b border-gray-800 px-6">
        <div className="flex items-center gap-2">
          <Settings className="h-5 w-5 text-gray-400" />
          <h2 className="text-sm font-medium text-gray-300">Settings</h2>
        </div>
      </header>

      <div className="flex-1 overflow-y-auto p-6">
        <div className="max-w-xl space-y-8">
          {/* API Connection */}
          <section>
            <h3 className="text-sm font-semibold text-gray-200 mb-4">
              Gateway Connection
            </h3>
            <div className="space-y-4">
              <div>
                <label className="block text-xs font-medium text-gray-400 mb-1.5">
                  API URL
                </label>
                <input
                  type="text"
                  value={apiUrl}
                  onChange={(e) => setApiUrl(e.target.value)}
                  className="w-full rounded-lg border border-gray-700 bg-gray-900 px-3 py-2 text-sm text-gray-200 outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500"
                  placeholder="http://localhost:8000"
                />
                <p className="mt-1 text-xs text-gray-600">
                  The URL where the databot gateway is running
                </p>
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-400 mb-1.5">
                  API Key
                </label>
                <input
                  type="password"
                  value={apiKey}
                  onChange={(e) => setApiKey(e.target.value)}
                  className="w-full rounded-lg border border-gray-700 bg-gray-900 px-3 py-2 text-sm text-gray-200 outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500"
                  placeholder="Optional — leave blank if auth is disabled"
                />
              </div>
            </div>
          </section>

          {/* About */}
          <section>
            <h3 className="text-sm font-semibold text-gray-200 mb-4">About</h3>
            <div className="rounded-xl border border-gray-800 bg-gray-900/50 p-5 space-y-3">
              <div className="flex justify-between">
                <span className="text-xs text-gray-500">Version</span>
                <span className="text-xs text-gray-300">0.2.0</span>
              </div>
              <div className="flex justify-between">
                <span className="text-xs text-gray-500">License</span>
                <span className="text-xs text-gray-300">MIT</span>
              </div>
              <div className="flex justify-between">
                <span className="text-xs text-gray-500">Source</span>
                <a
                  href="https://github.com/asb108/databot"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-xs text-indigo-400 hover:text-indigo-300"
                >
                  github.com/asb108/databot
                </a>
              </div>
            </div>
          </section>

          {/* Actions */}
          <div className="flex gap-3">
            <button
              onClick={handleSave}
              className="flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-500 transition-colors"
            >
              <Save className="h-4 w-4" />
              {saved ? "Saved!" : "Save Settings"}
            </button>
            <button
              onClick={handleReset}
              className="flex items-center gap-2 rounded-lg border border-gray-700 px-4 py-2 text-sm text-gray-400 hover:bg-gray-800 hover:text-gray-200 transition-colors"
            >
              <RotateCcw className="h-4 w-4" />
              Reset to Defaults
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
