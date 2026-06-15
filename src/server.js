"use strict";
require("dotenv").config();

const http = require("http");
const express = require("express");
const { v4: uuidv4 } = require("uuid");

const pool = require("./db");
const { enqueue, getQueueStats } = require("./queue");
const { createJob, getJob, listJobs, getAuditLogs } = require("./jobService");
const wsManager = require("./wsManager");
const { startWorkers } = require("./worker");

const app = express();
const PORT = process.env.PORT || 3000;

// ─── Middleware ───────────────────────────────────────────────────────────────

app.use(express.json());

// Basic request logger
app.use((req, _res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
  next();
});

// ─── Health check ─────────────────────────────────────────────────────────────

app.get("/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ status: "ok", db: "connected", uptime: process.uptime() });
  } catch (err) {
    res.status(503).json({ status: "degraded", db: "disconnected", error: err.message });
  }
});

// ─── Job Routes ───────────────────────────────────────────────────────────────

/**
 * POST /jobs
 * Enqueue a new job.
 * Body: { name: string, data: object, priority: "high"|"default"|"low" }
 */
app.post("/jobs", async (req, res) => {
  try {
    const { name, data = {}, priority = "default" } = req.body;

    if (!name) {
      return res.status(400).json({ error: "Job 'name' is required" });
    }
    if (!["high", "default", "low"].includes(priority)) {
      return res.status(400).json({ error: "priority must be 'high', 'default', or 'low'" });
    }

    const id = uuidv4();

    // 1. Persist job metadata to PostgreSQL
    const job = await createJob({ id, name, data, priority });

    // 2. Enqueue in BullMQ (Redis)
    const bullJob = await enqueue(name, data, priority, id);

    res.status(202).json({
      message: "Job enqueued",
      job: {
        id: job.id,
        name: job.name,
        priority: job.priority,
        status: job.status,
        bullJobId: bullJob.id,
        createdAt: job.created_at,
      },
    });
  } catch (err) {
    console.error("[POST /jobs]", err);
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /jobs
 * List jobs (optional ?status=queued|active|completed|failed|dead)
 */
app.get("/jobs", async (req, res) => {
  try {
    const { status, limit = 50, offset = 0 } = req.query;
    const jobs = await listJobs({ status, limit: parseInt(limit), offset: parseInt(offset) });
    res.json({ jobs, count: jobs.length });
  } catch (err) {
    console.error("[GET /jobs]", err);
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /jobs/:id
 * Fetch a single job status + audit trail.
 */
app.get("/jobs/:id", async (req, res) => {
  try {
    const job = await getJob(req.params.id);
    if (!job) return res.status(404).json({ error: "Job not found" });

    const auditLogs = await getAuditLogs(req.params.id);
    res.json({ job, auditLogs });
  } catch (err) {
    console.error("[GET /jobs/:id]", err);
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /queues/stats
 * Real-time counts from BullMQ (Redis).
 */
app.get("/queues/stats", async (req, res) => {
  try {
    const stats = await getQueueStats();
    res.json({ stats });
  } catch (err) {
    console.error("[GET /queues/stats]", err);
    res.status(500).json({ error: err.message });
  }
});

// ─── Bootstrap ────────────────────────────────────────────────────────────────

async function bootstrap() {
  // Verify DB connectivity
  try {
    await pool.query("SELECT 1");
    console.log("✅ PostgreSQL connected");
  } catch (err) {
    console.error("❌ PostgreSQL connection failed:", err.message);
    console.error("   Make sure PostgreSQL is running and run the migration: psql -f migrations/001_init.sql");
  }

  // Create HTTP server (needed to share with WebSocket)
  const server = http.createServer(app);

  // Attach WebSocket server
  wsManager.init(server);

  // Start BullMQ workers
  startWorkers();

  // Listen
  server.listen(PORT, () => {
    console.log(`\n🚀 Server running on http://localhost:${PORT}`);
    console.log(`🔌 WebSocket ready on  ws://localhost:${PORT}`);
    console.log(`📋 Endpoints:`);
    console.log(`   POST   /jobs              — enqueue a job`);
    console.log(`   GET    /jobs              — list jobs`);
    console.log(`   GET    /jobs/:id          — job status + audit trail`);
    console.log(`   GET    /queues/stats      — live queue counts`);
    console.log(`   GET    /health            — health check\n`);
  });
}

bootstrap();
