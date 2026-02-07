"use client";

import { useState, useEffect } from "react";
import { History, MessageSquare, Trash2, RefreshCw } from "lucide-react";
import { getSessions, getSessionHistory, deleteSession, type SessionInfo, type SessionHistory } from "@/lib/api";
import { formatRelativeTime } from "@/lib/utils";

export default function SessionsPage() {
  const [sessions, setSessions] = useState<SessionInfo[]>([]);
  const [selected, setSelected] = useState<SessionHistory | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchSessions = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await getSessions();
      setSessions(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load sessions");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchSessions();
  }, []);

  const handleSelect = async (key: string) => {
    try {
      const history = await getSessionHistory(key);
      setSelected(history);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load session");
    }
  };

  const handleDelete = async (key: string) => {
    if (!confirm("Delete this session?")) return;
    try {
      await deleteSession(key);
      setSessions((prev) => prev.filter((s) => s.key !== key));
      if (selected?.key === key) setSelected(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to delete session");
    }
  };

  return (
    <div className="flex h-full flex-col">
      <header className="flex h-14 items-center justify-between border-b border-gray-800 px-6">
        <div className="flex items-center gap-2">
          <History className="h-5 w-5 text-gray-400" />
          <h2 className="text-sm font-medium text-gray-300">Sessions</h2>
        </div>
        <button
          onClick={fetchSessions}
          className="flex items-center gap-1.5 rounded-lg border border-gray-700 px-3 py-1.5 text-xs text-gray-400 hover:bg-gray-800 hover:text-gray-200 transition-colors"
        >
          <RefreshCw className="h-3.5 w-3.5" />
          Refresh
        </button>
      </header>

      <div className="flex flex-1 overflow-hidden">
        {/* Session list */}
        <div className="w-80 border-r border-gray-800 overflow-y-auto">
          {loading ? (
            <div className="flex items-center justify-center py-12 text-sm text-gray-500">
              Loading sessions...
            </div>
          ) : error ? (
            <div className="px-4 py-8 text-center">
              <p className="text-sm text-red-400 mb-3">{error}</p>
              <p className="text-xs text-gray-500">
                Make sure the gateway is running at{" "}
                <code className="bg-gray-800 px-1 rounded">localhost:8000</code>
              </p>
            </div>
          ) : sessions.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-12 text-gray-500">
              <MessageSquare className="h-8 w-8 mb-2" />
              <p className="text-sm">No sessions yet</p>
            </div>
          ) : (
            sessions.map((session) => (
              <div
                key={session.key}
                onClick={() => handleSelect(session.key)}
                className={`flex items-center justify-between px-4 py-3 border-b border-gray-800/50 cursor-pointer hover:bg-gray-900 transition-colors ${
                  selected?.key === session.key ? "bg-gray-900" : ""
                }`}
              >
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-gray-300 truncate">
                    {session.key}
                  </p>
                  <div className="flex items-center gap-2 mt-1">
                    <span className="text-xs text-gray-500">
                      {session.message_count} messages
                    </span>
                    <span className="text-xs text-gray-600">•</span>
                    <span className="text-xs text-gray-500">
                      {formatRelativeTime(session.updated_at)}
                    </span>
                  </div>
                </div>
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    handleDelete(session.key);
                  }}
                  className="p-1.5 text-gray-600 hover:text-red-400 transition-colors"
                >
                  <Trash2 className="h-4 w-4" />
                </button>
              </div>
            ))
          )}
        </div>

        {/* Session detail */}
        <div className="flex-1 overflow-y-auto">
          {selected ? (
            <div className="p-6 space-y-4 max-w-3xl">
              <h3 className="text-sm font-medium text-gray-400 mb-4">
                Session: <span className="text-gray-200">{selected.key}</span>
              </h3>
              {selected.messages.map((msg, i) => (
                <div
                  key={i}
                  className={`rounded-lg p-4 ${
                    msg.role === "user"
                      ? "bg-gray-900 border border-gray-800"
                      : "bg-gray-900/50 border border-gray-800/50"
                  }`}
                >
                  <p className="text-xs font-medium text-gray-500 mb-2 uppercase">
                    {msg.role}
                  </p>
                  <p className="text-sm text-gray-300 whitespace-pre-wrap">
                    {msg.content}
                  </p>
                </div>
              ))}
            </div>
          ) : (
            <div className="flex h-full items-center justify-center text-sm text-gray-600">
              Select a session to view history
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
