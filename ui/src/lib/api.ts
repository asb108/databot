const API_BASE = process.env.NEXT_PUBLIC_API_URL || "";

export interface ChatRequest {
  message: string;
  sender?: string;
  chat_id?: string;
}

export interface ChatResponse {
  response: string;
  agent?: string;
}

export interface StreamEvent {
  type: "delta" | "tool_start" | "tool_result" | "done" | "error";
  data: string;
  tool_name: string;
}

export interface ConnectorInfo {
  name: string;
  type: string;
  connected: boolean;
  capabilities: string[];
}

export interface SessionInfo {
  key: string;
  created_at: string;
  updated_at: string;
  message_count: number;
}

export interface SessionHistory {
  key: string;
  messages: Array<{ role: string; content: string }>;
}

export interface HealthStatus {
  status: string;
  version: string;
  connectors: Record<string, string>;
}

export interface ToolInfo {
  name: string;
  description: string;
  parameters: Record<string, unknown>;
}

// ---------- Helpers ----------

function headers(apiKey?: string): HeadersInit {
  const h: HeadersInit = { "Content-Type": "application/json" };
  if (apiKey) h["X-API-Key"] = apiKey;
  return h;
}

// ---------- Chat ----------

export async function sendMessage(
  req: ChatRequest,
  apiKey?: string
): Promise<ChatResponse> {
  const res = await fetch(`${API_BASE}/api/v1/message`, {
    method: "POST",
    headers: headers(apiKey),
    body: JSON.stringify(req),
  });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

export async function* streamMessage(
  req: ChatRequest,
  apiKey?: string
): AsyncGenerator<StreamEvent> {
  const res = await fetch(`${API_BASE}/api/v1/stream`, {
    method: "POST",
    headers: headers(apiKey),
    body: JSON.stringify(req),
  });
  if (!res.ok) throw new Error(`API error: ${res.status}`);

  const reader = res.body?.getReader();
  if (!reader) throw new Error("No response body");

  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() || "";

    let eventType = "";
    for (const line of lines) {
      if (line.startsWith("event: ")) {
        eventType = line.slice(7).trim();
      } else if (line.startsWith("data: ")) {
        try {
          const parsed: StreamEvent = JSON.parse(line.slice(6));
          if (eventType) parsed.type = eventType as StreamEvent["type"];
          yield parsed;
        } catch {
          // skip malformed
        }
      }
    }
  }
}

// ---------- Health ----------

export async function getHealth(apiKey?: string): Promise<HealthStatus> {
  const res = await fetch(`${API_BASE}/health`, {
    headers: headers(apiKey),
  });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

// ---------- Connectors ----------

export async function getConnectors(
  apiKey?: string
): Promise<ConnectorInfo[]> {
  const res = await fetch(`${API_BASE}/api/v1/connectors`, {
    headers: headers(apiKey),
  });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  const data = await res.json();
  return data.connectors;
}

// ---------- Sessions ----------

export async function getSessions(
  apiKey?: string
): Promise<SessionInfo[]> {
  const res = await fetch(`${API_BASE}/api/v1/sessions`, {
    headers: headers(apiKey),
  });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  const data = await res.json();
  return data.sessions;
}

export async function getSessionHistory(
  key: string,
  apiKey?: string
): Promise<SessionHistory> {
  const res = await fetch(
    `${API_BASE}/api/v1/sessions/${encodeURIComponent(key)}`,
    { headers: headers(apiKey) }
  );
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

export async function deleteSession(
  key: string,
  apiKey?: string
): Promise<void> {
  const res = await fetch(
    `${API_BASE}/api/v1/sessions/${encodeURIComponent(key)}`,
    { method: "DELETE", headers: headers(apiKey) }
  );
  if (!res.ok) throw new Error(`API error: ${res.status}`);
}

// ---------- Tools ----------

export async function getTools(apiKey?: string): Promise<ToolInfo[]> {
  const res = await fetch(`${API_BASE}/api/v1/tools`, {
    headers: headers(apiKey),
  });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  const data = await res.json();
  return data.tools;
}
