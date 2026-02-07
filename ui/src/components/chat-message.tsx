"use client";

import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { Bot, User, Wrench, AlertCircle, ChevronDown, ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";
import { useState } from "react";

export interface ToolCall {
  name: string;
  args: string;
  result?: string;
}

export interface Message {
  id: string;
  role: "user" | "assistant";
  content: string;
  toolCalls?: ToolCall[];
  agent?: string;
  timestamp: Date;
}

function ToolCallBlock({ tool }: { tool: ToolCall }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="my-2 rounded-lg border border-gray-700 bg-gray-900/50">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex w-full items-center gap-2 px-3 py-2 text-sm text-gray-300 hover:bg-gray-800/50 rounded-lg transition-colors"
      >
        <Wrench className="h-4 w-4 text-amber-400" />
        <span className="font-medium text-amber-400">{tool.name}</span>
        {expanded ? (
          <ChevronDown className="ml-auto h-4 w-4" />
        ) : (
          <ChevronRight className="ml-auto h-4 w-4" />
        )}
      </button>
      {expanded && (
        <div className="border-t border-gray-700 px-3 py-2 space-y-2">
          <div>
            <p className="text-xs text-gray-500 mb-1">Arguments</p>
            <pre className="text-xs text-gray-400 bg-gray-800 rounded p-2 overflow-x-auto">
              {tool.args}
            </pre>
          </div>
          {tool.result && (
            <div>
              <p className="text-xs text-gray-500 mb-1">Result</p>
              <pre className="text-xs text-gray-400 bg-gray-800 rounded p-2 overflow-x-auto max-h-64 overflow-y-auto">
                {tool.result}
              </pre>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export function ChatMessage({ message }: { message: Message }) {
  const isUser = message.role === "user";

  return (
    <div
      className={cn(
        "flex gap-4 px-6 py-5",
        isUser ? "bg-transparent" : "bg-gray-900/30"
      )}
    >
      <div
        className={cn(
          "flex h-8 w-8 shrink-0 items-center justify-center rounded-lg",
          isUser ? "bg-gray-700" : "bg-indigo-600"
        )}
      >
        {isUser ? (
          <User className="h-4 w-4 text-gray-300" />
        ) : (
          <Bot className="h-4 w-4 text-white" />
        )}
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-1">
          <span className="text-sm font-medium text-gray-300">
            {isUser ? "You" : "Databot"}
          </span>
          {message.agent && (
            <span className="rounded-full bg-indigo-900/50 px-2 py-0.5 text-xs text-indigo-400">
              {message.agent}
            </span>
          )}
          <span className="text-xs text-gray-600">
            {message.timestamp.toLocaleTimeString()}
          </span>
        </div>

        {/* Tool calls */}
        {message.toolCalls?.map((tool, i) => (
          <ToolCallBlock key={i} tool={tool} />
        ))}

        {/* Content */}
        {message.content && (
          <div className="prose prose-invert prose-sm max-w-none text-gray-300">
            <ReactMarkdown remarkPlugins={[remarkGfm]}>
              {message.content}
            </ReactMarkdown>
          </div>
        )}
      </div>
    </div>
  );
}

export function StreamingIndicator() {
  return (
    <div className="flex gap-4 px-6 py-5 bg-gray-900/30">
      <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-indigo-600">
        <Bot className="h-4 w-4 text-white" />
      </div>
      <div className="flex items-center gap-1 pt-2">
        <div className="h-2 w-2 animate-bounce rounded-full bg-indigo-400 [animation-delay:0ms]" />
        <div className="h-2 w-2 animate-bounce rounded-full bg-indigo-400 [animation-delay:150ms]" />
        <div className="h-2 w-2 animate-bounce rounded-full bg-indigo-400 [animation-delay:300ms]" />
      </div>
    </div>
  );
}

export function ErrorMessage({ message }: { message: string }) {
  return (
    <div className="flex items-center gap-2 mx-6 my-2 rounded-lg border border-red-800/50 bg-red-950/30 px-4 py-3 text-sm text-red-400">
      <AlertCircle className="h-4 w-4 shrink-0" />
      {message}
    </div>
  );
}
