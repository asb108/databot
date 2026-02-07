"use client";

import { useState, useRef, useEffect, useCallback } from "react";
import { Plus, Sparkles } from "lucide-react";
import { ChatInput } from "@/components/chat-input";
import {
  ChatMessage,
  StreamingIndicator,
  ErrorMessage,
  type Message,
  type ToolCall,
} from "@/components/chat-message";
import { streamMessage } from "@/lib/api";

export default function ChatPage() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [isStreaming, setIsStreaming] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [chatId, setChatId] = useState(() => `chat-${Date.now()}`);
  const scrollRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = useCallback(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, []);

  useEffect(() => {
    scrollToBottom();
  }, [messages, isStreaming, scrollToBottom]);

  const handleSend = async (content: string) => {
    setError(null);
    const userMsg: Message = {
      id: `msg-${Date.now()}`,
      role: "user",
      content,
      timestamp: new Date(),
    };
    setMessages((prev) => [...prev, userMsg]);
    setIsStreaming(true);

    let assistantContent = "";
    const toolCalls: ToolCall[] = [];
    let currentAgent: string | undefined;

    try {
      const stream = streamMessage(
        { message: content, chat_id: chatId, sender: "ui" },
      );

      for await (const event of stream) {
        switch (event.type) {
          case "delta":
            assistantContent += event.data;
            break;
          case "tool_start":
            toolCalls.push({
              name: event.tool_name,
              args: event.data,
            });
            break;
          case "tool_result":
            if (toolCalls.length > 0) {
              toolCalls[toolCalls.length - 1].result = event.data;
            }
            break;
          case "done":
            assistantContent = event.data || assistantContent;
            break;
          case "error":
            setError(event.data);
            break;
        }

        // Live update assistant message
        setMessages((prev) => {
          const updated = [...prev];
          const lastMsg = updated[updated.length - 1];
          if (lastMsg?.role === "assistant") {
            updated[updated.length - 1] = {
              ...lastMsg,
              content: assistantContent,
              toolCalls: [...toolCalls],
              agent: currentAgent,
            };
          } else {
            updated.push({
              id: `msg-${Date.now()}-assistant`,
              role: "assistant",
              content: assistantContent,
              toolCalls: [...toolCalls],
              agent: currentAgent,
              timestamp: new Date(),
            });
          }
          return updated;
        });
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Connection failed");
    } finally {
      setIsStreaming(false);
    }
  };

  const handleNewChat = () => {
    setMessages([]);
    setError(null);
    setChatId(`chat-${Date.now()}`);
  };

  return (
    <div className="flex h-full flex-col">
      {/* Header */}
      <header className="flex h-14 items-center justify-between border-b border-gray-800 px-6">
        <h2 className="text-sm font-medium text-gray-300">Chat</h2>
        <button
          onClick={handleNewChat}
          className="flex items-center gap-2 rounded-lg border border-gray-700 px-3 py-1.5 text-xs text-gray-400 hover:bg-gray-800 hover:text-gray-200 transition-colors"
        >
          <Plus className="h-3.5 w-3.5" />
          New Chat
        </button>
      </header>

      {/* Messages */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto">
        {messages.length === 0 ? (
          <EmptyState />
        ) : (
          <div className="mx-auto max-w-4xl divide-y divide-gray-800/50">
            {messages.map((msg) => (
              <ChatMessage key={msg.id} message={msg} />
            ))}
            {isStreaming && messages[messages.length - 1]?.role === "user" && (
              <StreamingIndicator />
            )}
          </div>
        )}
        {error && <ErrorMessage message={error} />}
      </div>

      {/* Input */}
      <ChatInput onSend={handleSend} disabled={isStreaming} />
    </div>
  );
}

function EmptyState() {
  return (
    <div className="flex h-full flex-col items-center justify-center px-6">
      <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-indigo-600/10 mb-6">
        <Sparkles className="h-8 w-8 text-indigo-400" />
      </div>
      <h2 className="text-xl font-semibold text-gray-200 mb-2">
        How can I help you today?
      </h2>
      <p className="max-w-md text-center text-sm text-gray-500 mb-8">
        I can help you query databases, check pipeline status, monitor data quality,
        manage Kafka topics, and more.
      </p>
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 max-w-lg w-full">
        {[
          "Show me the top 10 tables by row count",
          "What Airflow DAGs failed today?",
          "Check consumer group lag on Kafka",
          "Run a data quality check on orders table",
        ].map((prompt) => (
          <button
            key={prompt}
            className="rounded-xl border border-gray-800 bg-gray-900/50 px-4 py-3 text-left text-sm text-gray-400 hover:border-gray-700 hover:bg-gray-900 hover:text-gray-300 transition-all"
          >
            {prompt}
          </button>
        ))}
      </div>
    </div>
  );
}
