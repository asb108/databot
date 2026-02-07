"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  MessageSquare,
  Database,
  History,
  Activity,
  Settings,
  Bot,
  Wrench,
} from "lucide-react";
import { cn } from "@/lib/utils";

const navigation = [
  { name: "Chat", href: "/", icon: MessageSquare },
  { name: "Sessions", href: "/sessions", icon: History },
  { name: "Connectors", href: "/connectors", icon: Database },
  { name: "Tools", href: "/tools", icon: Wrench },
  { name: "Status", href: "/status", icon: Activity },
  { name: "Settings", href: "/settings", icon: Settings },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="flex w-64 flex-col border-r border-gray-800 bg-gray-950">
      {/* Logo */}
      <div className="flex h-16 items-center gap-3 border-b border-gray-800 px-6">
        <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-indigo-600">
          <Bot className="h-5 w-5 text-white" />
        </div>
        <div>
          <h1 className="text-lg font-semibold text-white">Databot</h1>
          <p className="text-xs text-gray-500">AI Data Assistant</p>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 space-y-1 px-3 py-4">
        {navigation.map((item) => {
          const isActive =
            pathname === item.href ||
            (item.href !== "/" && pathname.startsWith(item.href));
          return (
            <Link
              key={item.name}
              href={item.href}
              className={cn(
                "flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm font-medium transition-colors",
                isActive
                  ? "bg-gray-800 text-white"
                  : "text-gray-400 hover:bg-gray-900 hover:text-gray-200"
              )}
            >
              <item.icon className="h-5 w-5" />
              {item.name}
            </Link>
          );
        })}
      </nav>

      {/* Version */}
      <div className="border-t border-gray-800 px-6 py-4">
        <p className="text-xs text-gray-600">v0.2.0</p>
      </div>
    </aside>
  );
}
