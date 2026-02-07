"use client";

import { useState, useEffect } from "react";
import {
  Database,
  RefreshCw,
  CheckCircle,
  XCircle,
  AlertTriangle,
  Unplug,
} from "lucide-react";
import { getConnectors, getHealth, type ConnectorInfo, type HealthStatus } from "@/lib/api";
import { cn } from "@/lib/utils";

function StatusIcon({ status }: { status: string }) {
  switch (status) {
    case "healthy":
      return <CheckCircle className="h-5 w-5 text-emerald-400" />;
    case "degraded":
      return <AlertTriangle className="h-5 w-5 text-amber-400" />;
    case "unreachable":
      return <XCircle className="h-5 w-5 text-red-400" />;
    default:
      return <Unplug className="h-5 w-5 text-gray-500" />;
  }
}

function statusColor(status: string) {
  switch (status) {
    case "healthy":
      return "text-emerald-400 bg-emerald-400/10 border-emerald-400/20";
    case "degraded":
      return "text-amber-400 bg-amber-400/10 border-amber-400/20";
    case "unreachable":
      return "text-red-400 bg-red-400/10 border-red-400/20";
    default:
      return "text-gray-500 bg-gray-500/10 border-gray-500/20";
  }
}

export default function ConnectorsPage() {
  const [connectors, setConnectors] = useState<ConnectorInfo[]>([]);
  const [health, setHealth] = useState<HealthStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const [connData, healthData] = await Promise.all([
        getConnectors(),
        getHealth(),
      ]);
      setConnectors(connData);
      setHealth(healthData);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load connectors");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  const connectorStatuses = health?.connectors || {};

  return (
    <div className="flex h-full flex-col">
      <header className="flex h-14 items-center justify-between border-b border-gray-800 px-6">
        <div className="flex items-center gap-2">
          <Database className="h-5 w-5 text-gray-400" />
          <h2 className="text-sm font-medium text-gray-300">Connectors</h2>
        </div>
        <button
          onClick={fetchData}
          className="flex items-center gap-1.5 rounded-lg border border-gray-700 px-3 py-1.5 text-xs text-gray-400 hover:bg-gray-800 hover:text-gray-200 transition-colors"
        >
          <RefreshCw className="h-3.5 w-3.5" />
          Refresh
        </button>
      </header>

      <div className="flex-1 overflow-y-auto p-6">
        {loading ? (
          <div className="flex items-center justify-center py-12 text-sm text-gray-500">
            Loading connectors...
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
          <>
            {/* Health Summary */}
            {health && (
              <div className="mb-8 flex items-center gap-4 rounded-xl border border-gray-800 bg-gray-900/50 p-4">
                <div className="flex h-12 w-12 items-center justify-center rounded-lg bg-indigo-600/10">
                  <Database className="h-6 w-6 text-indigo-400" />
                </div>
                <div>
                  <p className="text-sm font-medium text-gray-200">
                    System Status:{" "}
                    <span
                      className={cn(
                        "capitalize",
                        health.status === "ok"
                          ? "text-emerald-400"
                          : "text-amber-400"
                      )}
                    >
                      {health.status}
                    </span>
                  </p>
                  <p className="text-xs text-gray-500">
                    Version {health.version} •{" "}
                    {Object.keys(connectorStatuses).length} connectors configured
                  </p>
                </div>
              </div>
            )}

            {/* Connector Cards */}
            {connectors.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-16 text-gray-500">
                <Unplug className="h-12 w-12 mb-3" />
                <p className="text-sm font-medium">No connectors configured</p>
                <p className="text-xs mt-1">
                  Add connectors in your databot.yaml config file
                </p>
              </div>
            ) : (
              <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
                {connectors.map((conn) => {
                  const status = connectorStatuses[conn.name] || "not_configured";
                  return (
                    <div
                      key={conn.name}
                      className="rounded-xl border border-gray-800 bg-gray-900/50 p-5 hover:border-gray-700 transition-colors"
                    >
                      <div className="flex items-start justify-between mb-4">
                        <div>
                          <h3 className="text-sm font-semibold text-gray-200">
                            {conn.name}
                          </h3>
                          <span
                            className={cn(
                              "mt-1 inline-block rounded-full border px-2 py-0.5 text-xs",
                              statusColor(status)
                            )}
                          >
                            {status}
                          </span>
                        </div>
                        <StatusIcon status={status} />
                      </div>
                      <div className="space-y-2">
                        <div className="flex items-center justify-between">
                          <span className="text-xs text-gray-500">Type</span>
                          <span className="text-xs font-medium text-gray-300 uppercase">
                            {conn.type}
                          </span>
                        </div>
                        <div className="flex items-center justify-between">
                          <span className="text-xs text-gray-500">Connected</span>
                          <span
                            className={cn(
                              "text-xs font-medium",
                              conn.connected ? "text-emerald-400" : "text-gray-500"
                            )}
                          >
                            {conn.connected ? "Yes" : "No"}
                          </span>
                        </div>
                      </div>
                      {conn.capabilities.length > 0 && (
                        <div className="mt-4 flex flex-wrap gap-1.5">
                          {conn.capabilities.map((cap) => (
                            <span
                              key={cap}
                              className="rounded-md bg-gray-800 px-2 py-0.5 text-xs text-gray-400"
                            >
                              {cap}
                            </span>
                          ))}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
