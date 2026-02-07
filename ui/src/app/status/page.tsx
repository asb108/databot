"use client";

import { useState, useEffect } from "react";
import {
  Activity,
  RefreshCw,
  CheckCircle,
  XCircle,
  Server,
  Clock,
} from "lucide-react";
import { getHealth, type HealthStatus } from "@/lib/api";
import { cn } from "@/lib/utils";

export default function StatusPage() {
  const [health, setHealth] = useState<HealthStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastChecked, setLastChecked] = useState<Date | null>(null);

  const fetchHealth = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await getHealth();
      setHealth(data);
      setLastChecked(new Date());
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to connect");
      setHealth(null);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchHealth();
    const interval = setInterval(fetchHealth, 30000); // Auto-refresh every 30s
    return () => clearInterval(interval);
  }, []);

  const connectors = health?.connectors || {};
  const healthyCount = Object.values(connectors).filter(
    (s) => s === "healthy"
  ).length;
  const totalCount = Object.keys(connectors).length;

  return (
    <div className="flex h-full flex-col">
      <header className="flex h-14 items-center justify-between border-b border-gray-800 px-6">
        <div className="flex items-center gap-2">
          <Activity className="h-5 w-5 text-gray-400" />
          <h2 className="text-sm font-medium text-gray-300">System Status</h2>
        </div>
        <div className="flex items-center gap-3">
          {lastChecked && (
            <span className="flex items-center gap-1 text-xs text-gray-600">
              <Clock className="h-3 w-3" />
              Last checked: {lastChecked.toLocaleTimeString()}
            </span>
          )}
          <button
            onClick={fetchHealth}
            className="flex items-center gap-1.5 rounded-lg border border-gray-700 px-3 py-1.5 text-xs text-gray-400 hover:bg-gray-800 hover:text-gray-200 transition-colors"
          >
            <RefreshCw className="h-3.5 w-3.5" />
            Check Now
          </button>
        </div>
      </header>

      <div className="flex-1 overflow-y-auto p-6">
        {/* Gateway Status */}
        <div className="mb-8 rounded-xl border border-gray-800 bg-gray-900/50 p-6">
          <div className="flex items-center gap-4">
            <div
              className={cn(
                "flex h-14 w-14 items-center justify-center rounded-xl",
                error
                  ? "bg-red-400/10"
                  : health?.status === "ok"
                  ? "bg-emerald-400/10"
                  : "bg-amber-400/10"
              )}
            >
              {error ? (
                <XCircle className="h-7 w-7 text-red-400" />
              ) : (
                <Server
                  className={cn(
                    "h-7 w-7",
                    health?.status === "ok"
                      ? "text-emerald-400"
                      : "text-amber-400"
                  )}
                />
              )}
            </div>
            <div>
              <h3 className="text-lg font-semibold text-gray-200">
                Gateway{" "}
                {error ? (
                  <span className="text-red-400">Offline</span>
                ) : (
                  <span
                    className={cn(
                      health?.status === "ok"
                        ? "text-emerald-400"
                        : "text-amber-400"
                    )}
                  >
                    {health?.status === "ok" ? "Online" : "Degraded"}
                  </span>
                )}
              </h3>
              {error ? (
                <p className="text-sm text-red-400">{error}</p>
              ) : (
                <p className="text-sm text-gray-500">
                  Version {health?.version} • {healthyCount}/{totalCount}{" "}
                  connectors healthy
                </p>
              )}
            </div>
          </div>
        </div>

        {/* Summary Cards */}
        {!error && health && (
          <>
            <div className="grid grid-cols-1 gap-4 mb-8 sm:grid-cols-3">
              <StatusCard
                label="Connectors"
                value={totalCount.toString()}
                subtext={`${healthyCount} healthy`}
                ok={healthyCount === totalCount}
              />
              <StatusCard
                label="Gateway"
                value={health.status === "ok" ? "Healthy" : "Degraded"}
                subtext="Auto-checking every 30s"
                ok={health.status === "ok"}
              />
              <StatusCard
                label="Version"
                value={health.version}
                subtext="databot-ai"
                ok={true}
              />
            </div>

            {/* Connector Details */}
            <h3 className="text-sm font-medium text-gray-400 mb-3">
              Connector Health
            </h3>
            <div className="space-y-2">
              {Object.entries(connectors).map(([name, status]) => (
                <div
                  key={name}
                  className="flex items-center justify-between rounded-lg border border-gray-800 bg-gray-900/50 px-5 py-3"
                >
                  <div className="flex items-center gap-3">
                    {status === "healthy" ? (
                      <CheckCircle className="h-4 w-4 text-emerald-400" />
                    ) : status === "degraded" ? (
                      <Activity className="h-4 w-4 text-amber-400" />
                    ) : (
                      <XCircle className="h-4 w-4 text-red-400" />
                    )}
                    <span className="text-sm font-medium text-gray-300">
                      {name}
                    </span>
                  </div>
                  <span
                    className={cn(
                      "rounded-full px-2.5 py-0.5 text-xs font-medium",
                      status === "healthy"
                        ? "bg-emerald-400/10 text-emerald-400"
                        : status === "degraded"
                        ? "bg-amber-400/10 text-amber-400"
                        : "bg-red-400/10 text-red-400"
                    )}
                  >
                    {status}
                  </span>
                </div>
              ))}
              {totalCount === 0 && (
                <p className="text-sm text-gray-600 text-center py-4">
                  No connectors configured
                </p>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

function StatusCard({
  label,
  value,
  subtext,
  ok,
}: {
  label: string;
  value: string;
  subtext: string;
  ok: boolean;
}) {
  return (
    <div className="rounded-xl border border-gray-800 bg-gray-900/50 p-5">
      <p className="text-xs text-gray-500 mb-1">{label}</p>
      <p
        className={cn(
          "text-2xl font-semibold",
          ok ? "text-gray-200" : "text-amber-400"
        )}
      >
        {value}
      </p>
      <p className="text-xs text-gray-600 mt-1">{subtext}</p>
    </div>
  );
}
