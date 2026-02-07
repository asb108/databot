# Databot UI

Web dashboard for the Databot AI data assistant. Built with Next.js, TypeScript, and Tailwind CSS.

## Features

- **Chat** — Real-time conversation with SSE streaming, tool call visualization, markdown rendering
- **Sessions** — Browse and manage conversation history
- **Connectors** — View connector status, health, and capabilities
- **Tools** — Explore registered tools and their parameter schemas
- **Status** — Live system health dashboard with auto-refresh
- **Settings** — Configure gateway URL and API key

## Getting Started

1. Start the databot gateway:

```bash
databot gateway --port 18790
```

2. Start the UI dev server:

```bash
cd ui
npm install
npm run dev
```

3. Open [http://localhost:3000](http://localhost:3000)

The Next.js rewrites proxy routes API calls to `localhost:18790` by default. To change, set `NEXT_PUBLIC_API_URL` in `.env.local`.

## Build

```bash
npm run build
npm start
```
