"use client";

import { useState, useEffect } from "react";
import { Wrench, RefreshCw, ChevronDown, ChevronRight } from "lucide-react";
import { getTools, type ToolInfo } from "@/lib/api";

export default function ToolsPage() {
  const [tools, setTools] = useState<ToolInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  const fetchTools = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await getTools();
      setTools(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load tools");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchTools();
  }, []);

  const toggleExpand = (name: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  };

  return (
    <div className="flex h-full flex-col">
      <header className="flex h-14 items-center justify-between border-b border-gray-800 px-6">
        <div className="flex items-center gap-2">
          <Wrench className="h-5 w-5 text-gray-400" />
          <h2 className="text-sm font-medium text-gray-300">
            Tools{" "}
            {!loading && (
              <span className="text-gray-600">({tools.length})</span>
            )}
          </h2>
        </div>
        <button
          onClick={fetchTools}
          className="flex items-center gap-1.5 rounded-lg border border-gray-700 px-3 py-1.5 text-xs text-gray-400 hover:bg-gray-800 hover:text-gray-200 transition-colors"
        >
          <RefreshCw className="h-3.5 w-3.5" />
          Refresh
        </button>
      </header>

      <div className="flex-1 overflow-y-auto p-6">
        {loading ? (
          <div className="flex items-center justify-center py-12 text-sm text-gray-500">
            Loading tools...
          </div>
        ) : error ? (
          <div className="text-center py-12">
            <p className="text-sm text-red-400 mb-3">{error}</p>
            <p className="text-xs text-gray-500">
              Make sure the gateway is running at{" "}
              <code className="bg-gray-800 px-1 rounded">localhost:8000</code>
            </p>
          </div>
        ) : (
          <div className="max-w-3xl space-y-2">
            {tools.map((tool) => (
              <div
                key={tool.name}
                className="rounded-xl border border-gray-800 bg-gray-900/50 overflow-hidden"
              >
                <button
                  onClick={() => toggleExpand(tool.name)}
                  className="flex w-full items-center gap-3 px-5 py-4 text-left hover:bg-gray-800/30 transition-colors"
                >
                  <Wrench className="h-4 w-4 text-indigo-400 shrink-0" />
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium text-gray-200">
                      {tool.name}
                    </p>
                    <p className="text-xs text-gray-500 truncate mt-0.5">
                      {tool.description}
                    </p>
                  </div>
                  {expanded.has(tool.name) ? (
                    <ChevronDown className="h-4 w-4 text-gray-500" />
                  ) : (
                    <ChevronRight className="h-4 w-4 text-gray-500" />
                  )}
                </button>
                {expanded.has(tool.name) && (
                  <div className="border-t border-gray-800 px-5 py-4">
                    <p className="text-xs text-gray-500 mb-2">Description</p>
                    <p className="text-sm text-gray-300 mb-4">
                      {tool.description}
                    </p>
                    <p className="text-xs text-gray-500 mb-2">Parameter Schema</p>
                    <pre className="text-xs text-gray-400 bg-gray-800 rounded-lg p-3 overflow-x-auto">
                      {JSON.stringify(tool.parameters, null, 2)}
                    </pre>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
