# GTM Engine: Closed-Loop Sales Conversation Memory & Outbound Agent

[![Build Status](https://img.shields.io/github/actions/workflow/status/Rajitha-SL/gtm-conversation-memory-agent/ci.yml?branch=main&style=flat-square)](https://github.com/Rajitha-SL/gtm-conversation-memory-agent/actions)
[![Node.js Version](https://img.shields.io/badge/node-v20-blue?style=flat-square&logo=node.js)](https://nodejs.org/)
[![Next.js Version](https://img.shields.io/badge/next.js-v15%2F16-black?style=flat-square&logo=next.js)](https://nextjs.org/)
[![PostgreSQL](https://img.shields.io/badge/postgresql-16-blue?style=flat-square&logo=postgresql)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/redis-7-red?style=flat-square&logo=redis)](https://redis.io/)
[![License](https://img.shields.io/badge/license-MIT-green?style=flat-square)](LICENSE)

An event-driven, automated pipeline that ingests post-call transcripts, utilizes Gemini LLM to extract target persona pain points and objections into a structured PostgreSQL database, and triggers external GTM automation (e.g. Clay) for outbound sales campaigns.

---

## 1. System Architecture

The GTM Context Engine orchestrates raw sales transcripts into highly actionable market outreach coordinates:

![System Architecture Diagram](architecture-diagram.png)

### Technology Stack
- **API Entry Layer**: Fastify (Node.js) web server serving ingestion hooks, SSE, and health check routes.
- **Queue & Event Broker**: BullMQ backplane powered by Redis, handling backoff retries and execution loops.
- **Background Worker**: Asynchronous Node.js job parser powered by the Google Gemini Core API (`gemini-2.5-flash`).
- **Database & Data Memory**: PostgreSQL database managed via Prisma ORM for relational persistence.
- **Control Dashboard**: Next.js React frontend rendering a real-time table matrix using Server-Sent Events (SSE) streaming.

---

## 2. Key Features

- ⚡ **Real-Time Data Engine**: Server-Sent Events (SSE) paired with PostgreSQL `LISTEN/NOTIFY` triggers dynamically broadcast database updates instantly to connected clients without HTTP page polling.
- 📊 **Executive Matrix UI**: Sleek, glassmorphism-styled Next.js dashboard equipped with case-insensitive search, status tabs, and real-time calculated metrics (Total Ingested, Success Rate, Active, and Failures).
- 🔄 **Self-Healing Retries**: One-click "Retry AI Analysis" button inside the details drawer to manually re-enqueue faulted jobs directly back into the BullMQ pipeline.
- 🛡️ **Production Hardening**: Enforces route-level rate limiting, strict JSON request schema validations, structured JSON logging (Pino), heartbeat keep-alives (`: ping\n\n`), and clean SIGINT/SIGTERM graceful shutdowns.
- 🔌 **Activation Layer**: Parses structured summaries to isolate tech stack keywords, contact emails, buying intent scores, and GTM insights, dispatching them automatically to outbound outreach sequence streams.

---

## 3. Quickstart Guide

### Option A: Local Run via Docker Compose (Recommended)
Spin up the entire stack—PostgreSQL database, Redis queue, Fastify server, background worker, and Next.js frontend—with a single command:

1. Clone the repository and configure your variables in the root `.env`:
   ```bash
   DATABASE_URL="postgresql://gtm_admin:admin_secure_pass123@database:5432/gtm_memory_engine?schema=public"
   GEMINI_API_KEY="your-gemini-api-key"
   ```
2. Build and start the containers:
   ```bash
   docker compose up --build
   ```
3. Open http://localhost:3001 in your browser to access the dashboard.

### Option B: Local Run via npm Scripts
Ensure you have Node.js v20+, PostgreSQL, and Redis running locally:

1. **Configure Environment Variables**:
   Create a `.env` file in the `backend/` and `frontend/` directories following the templates.
2. **Backend Setup**:
   ```bash
   cd backend
   npm install
   npx prisma db push
   npx prisma generate
   npm run start   # Launches Fastify API Server on port 3000
   ```
3. **Worker Setup** (In a separate terminal):
   ```bash
   cd backend
   npm run worker  # Starts BullMQ worker thread
   ```
4. **Frontend Setup** (In a separate terminal):
   ```bash
   cd frontend
   npm install
   npm run dev     # Starts Next.js Dev Server on http://localhost:3001
   ```

---

## 4. API Reference

### `POST /api/v1/webhooks/gong`
Ingests a call transcript payload and enqueues a job for analysis.
- **Request Body Schema**:
  ```json
  {
    "callId": "call_test_100",
    "rawTranscript": "DevOps Lead: We want to migrate off AWS, but the $150k cost is currently a blocker..."
  }
  ```
- **Responses**:
  - `202 Accepted`: Job enqueued successfully.
  - `400 Bad Request`: Validation failure (missing required fields).
  - `429 Too Many Requests`: Rate limit exceeded.

### `GET /api/v1/jobs/stream`
Establishes a persistent Server-Sent Events (SSE) stream broadcasting real-time job changes.

### `POST /api/v1/jobs/:id/retry`
Resets a faulted job's status to `PROCESSING` and pushes it back into the BullMQ worker queue.

### `GET /api/v1/health`
Returns connection health checks for PostgreSQL and Redis alongside active BullMQ queue job counts:
```json
{
  "status": "UP",
  "uptime": 128.45,
  "timestamp": "2026-07-17T09:18:00.000Z",
  "services": {
    "database": "UP",
    "redis": "UP"
  },
  "queueMetrics": {
    "active": 0,
    "completed": 12,
    "failed": 2
  }
}
```

---

## 5. Development Specs
Detailed API payload schemas and structural callbacks can be found in the [docs/COMPONENT_SPECS.md](docs/COMPONENT_SPECS.md) specifications sheet.
